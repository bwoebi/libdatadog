// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

pub mod api;
pub mod internal;
pub mod pprof;
pub mod profiled_endpoints;

use std::borrow::Cow;
use std::convert::TryInto;
use std::num::NonZeroI64;
use std::time::{Duration, SystemTime};

use crate::collections::identifiable::*;
use internal::*;
use profiled_endpoints::ProfiledEndpointsStats;
use prost::{EncodeError, Message};

use self::api::UpscalingInfo;

pub type Timestamp = NonZeroI64;
pub type TimestampedObservation = (Timestamp, Box<[i64]>);

pub struct Profile {
    endpoints: Endpoints,
    functions: Table<Function>,
    labels: FxIndexSet<Label>,
    label_sets: FxIndexSet<LabelSet>,
    locations: Table<Location>,
    mappings: Table<Mapping>,
    observations: Observations,
    period: Option<(i64, ValueType)>,
    sample_types: Vec<ValueType>,
    stack_traces: FxIndexSet<StackTrace>,
    start_time: SystemTime,
    strings: StringTable,
    timestamp_key: StringId,
    upscaling_rules: UpscalingRules,
    limits: api::Limits,
}

#[derive(Default)]
pub struct ProfileBuilder<'a> {
    period: Option<api::Period<'a>>,
    sample_types: Vec<api::ValueType<'a>>,
    start_time: Option<SystemTime>,
    limits: Option<api::Limits>,
}

impl<'a> ProfileBuilder<'a> {
    pub const fn new() -> Self {
        ProfileBuilder {
            period: None,
            sample_types: Vec::new(),
            start_time: None,
            limits: None,
        }
    }

    pub fn period(mut self, period: Option<api::Period<'a>>) -> Self {
        self.period = period;
        self
    }

    pub fn sample_types(mut self, sample_types: Vec<api::ValueType<'a>>) -> Self {
        self.sample_types = sample_types;
        self
    }

    pub fn start_time(mut self, start_time: Option<SystemTime>) -> Self {
        self.start_time = start_time;
        self
    }

    pub fn limits(mut self, limits: api::Limits) -> Self {
        self.limits = Some(limits);
        self
    }

    pub fn build(self) -> anyhow::Result<Profile> {
        let limits = match self.limits {
            Some(l) => l,
            None => anyhow::bail!("profile limits are required but were not given"),
        };
        let mut profile = Profile::new(self.start_time.unwrap_or_else(SystemTime::now), limits)?;

        let sample_types: Result<Vec<ValueType>, _> = self
            .sample_types
            .iter()
            .map(|vt| -> anyhow::Result<ValueType> {
                Ok(ValueType {
                    r#type: profile.intern(vt.r#type)?,
                    unit: profile.intern(vt.unit)?,
                })
            })
            .collect();

        profile.sample_types = sample_types?;

        if let Some(period) = self.period {
            profile.period = Some((
                period.value,
                ValueType {
                    r#type: profile.intern(period.r#type.r#type)?,
                    unit: profile.intern(period.r#type.unit)?,
                },
            ));
        };

        Ok(profile)
    }
}

pub struct EncodedProfile {
    pub start: SystemTime,
    pub end: SystemTime,
    pub buffer: Vec<u8>,
    pub endpoints_stats: ProfiledEndpointsStats,
}

// For testing and debugging purposes
impl Profile {
    pub fn only_for_testing_num_aggregated_samples(&self) -> usize {
        self.observations
            .iter()
            .filter(|(_, ts, _)| ts.is_none())
            .count()
    }

    pub fn only_for_testing_num_timestamped_samples(&self) -> usize {
        use std::collections::HashSet;
        let sample_set: HashSet<Timestamp> =
            HashSet::from_iter(self.observations.iter().filter_map(|(_, ts, _)| ts));
        sample_set.len()
    }
}

impl Profile {
    /// Creates a profile with `start_time`.
    /// Initializes the string table to hold:
    ///  - "" (the empty string)
    ///  - "local root span id"
    ///  - "trace endpoint"
    ///  - "end_timestamp_ns"
    /// All other fields are default.
    pub fn new(start_time: SystemTime, limits: api::Limits) -> anyhow::Result<Self> {
        /* Do not use Profile's default() impl here or it will cause a stack
         * overflow, since that default impl calls this method.
         */
        let mut profile = Self {
            endpoints: Default::default(),
            labels: Default::default(),
            label_sets: Default::default(),
            observations: Default::default(),
            period: None,
            sample_types: vec![],
            stack_traces: Default::default(),
            start_time,
            functions: Table::with_arena_capacity(limits.functions_mem.get())?,
            locations: Table::with_arena_capacity(limits.locations_mem.get())?,
            mappings: Table::with_arena_capacity(limits.mappings_mem.get())?,
            strings: StringTable::with_capacity(limits.strings_mem.get())?,
            timestamp_key: Default::default(),
            upscaling_rules: Default::default(),
            limits,
        };

        // Ensure the empty string is the first inserted item and has a 0 id.
        let _id = profile.intern("")?;
        debug_assert!(_id == StringId::ZERO);

        profile.endpoints.local_root_span_id_label = profile.intern("local root span id")?;
        profile.endpoints.endpoint_label = profile.intern("trace endpoint")?;
        profile.timestamp_key = profile.intern("end_timestamp_ns")?;
        Ok(profile)
    }

    #[cfg(test)]
    fn interned_strings_count(&self) -> usize {
        self.strings.len()
    }

    /// Interns the `str` as a string, returning the id in the string table.
    /// The empty string is guaranteed to have an id of [StringId::ZERO].
    fn intern(&mut self, item: &str) -> anyhow::Result<StringId> {
        Ok(self.strings.insert(item)?)
    }

    pub fn builder<'a>() -> ProfileBuilder<'a> {
        ProfileBuilder::new()
    }

    fn add_stacktrace(&mut self, locations: Vec<LocationId>) -> StackTraceId {
        self.stack_traces.dedup(StackTrace { locations })
    }

    fn get_stacktrace(&self, st: StackTraceId) -> &StackTrace {
        self.stack_traces
            .get_index(st.to_raw_id())
            .expect("StackTraceId {st} to exist in profile")
    }

    fn add_function(&mut self, function: &api::Function) -> anyhow::Result<FunctionId> {
        let name = self.intern(function.name)?;
        let system_name = self.intern(function.system_name)?;
        let filename = self.intern(function.filename)?;

        let start_line = function.start_line;
        Ok(self.functions.insert(Function {
            name,
            system_name,
            filename,
            start_line,
        })?)
    }

    fn add_location(&mut self, location: &api::Location) -> anyhow::Result<LocationId> {
        let mapping_id = self.add_mapping(&location.mapping);
        let function_id = self.add_function(&location.function);
        Ok(self.locations.insert(Location {
            mapping_id: mapping_id?,
            function_id: function_id?,
            address: location.address,
            line: location.line,
        })?)
    }

    fn add_mapping(&mut self, mapping: &api::Mapping) -> anyhow::Result<MappingId> {
        let filename = self.intern(mapping.filename);
        let build_id = self.intern(mapping.build_id);

        Ok(self.mappings.insert(Mapping {
            memory_start: mapping.memory_start,
            memory_limit: mapping.memory_limit,
            file_offset: mapping.file_offset,
            filename: filename?,
            build_id: build_id?,
        })?)
    }

    pub fn add(&mut self, sample: api::Sample) -> anyhow::Result<()> {
        anyhow::ensure!(
            sample.values.len() == self.sample_types.len(),
            "expected {} sample types, but sample had {} sample types",
            self.sample_types.len(),
            sample.values.len(),
        );

        let (labels, timestamp) = self.extract_sample_labels(&sample)?;

        let locations = sample
            .locations
            .iter()
            .map(|l| self.add_location(l))
            .collect::<Result<Vec<_>, _>>();

        let stacktrace = self.add_stacktrace(locations?);
        self.observations
            .add(Sample::new(labels, stacktrace), timestamp, sample.values);
        Ok(())
    }

    /// Validates labels and converts them to the internal representation.
    /// Extracts out the timestamp label, if it exists.
    fn extract_timestamp(&mut self, sample: &api::Sample) -> anyhow::Result<Option<Timestamp>> {
        if let Some(label) = sample
            .labels
            .iter()
            .find(|label| label.key == "end_timestamp_ns")
        {
            anyhow::ensure!(
                label.str.is_none(),
                "the label \"{}\" must be sent as a number, not string {}",
                label.str.unwrap(),
                label.key
            );
            anyhow::ensure!(label.num != 0, "the label \"{}\" must not be 0", label.key);
            anyhow::ensure!(label.num_unit.is_none(), "Timestamps with label '{}' are always nanoseconds and do not take a unit: found '{}'", label.key, label.num_unit.unwrap());

            Ok(Some(NonZeroI64::new(label.num).unwrap()))
        } else {
            Ok(None)
        }
    }

    /// Validates labels and converts them to the internal representation.
    /// Extracts out the timestamp label, if it exists.
    fn extract_sample_labels(
        &mut self,
        sample: &api::Sample,
    ) -> anyhow::Result<(LabelSetId, Option<Timestamp>)> {
        let timestamp = self.extract_timestamp(sample)?;

        let mut labels: Vec<LabelId> = Vec::with_capacity(if timestamp.is_some() {
            sample.labels.len() - 1
        } else {
            sample.labels.len()
        });
        let mut local_root_span_id_label = None;

        for label in sample.labels.iter() {
            if label.key == "end_timestamp_ns" {
                continue;
            }

            let key = self.intern(label.key)?;
            let internal_label = if let Some(s) = label.str {
                let str = self.intern(s)?;
                Label::str(key, str)
            } else {
                let num = label.num;
                let num_unit = match label.num_unit {
                    None => None,
                    Some(s) => Some(self.intern(s)?),
                };
                Label::num(key, num, num_unit)
            };

            let label_id = self.labels.dedup(internal_label);

            if key == self.endpoints.local_root_span_id_label {
                anyhow::ensure!(
                    local_root_span_id_label.is_none(),
                    "only one label per sample can have the key \"local root span id\", found two: {:?}, {:?}",
                    self.get_label(local_root_span_id_label.unwrap()), label
                );

                // Panic: if the label.str isn't 0, then str must have been provided.
                anyhow::ensure!(
                    label.str.is_none(),
                    "the label \"local root span id\" must be sent as a number, not string {}",
                    label.str.unwrap()
                );
                anyhow::ensure!(
                    label.num != 0,
                    "the label \"local root span id\" must not be 0"
                );
                local_root_span_id_label = Some(label_id);
            }

            labels.push(label_id);
        }

        let label_set_id = self.label_sets.dedup(LabelSet::new(labels));

        Ok((label_set_id, timestamp))
    }

    fn extract_api_sample_types(&self) -> Vec<api::ValueType> {
        self.sample_types
            .iter()
            .map(|sample_type| api::ValueType {
                r#type: self.get_string(sample_type.r#type),
                unit: self.get_string(sample_type.unit),
            })
            .collect()
    }

    /// Resets all data except the sample types and period. Returns the
    /// previous Profile on success.
    pub fn reset(&mut self, start_time: Option<SystemTime>) -> anyhow::Result<Profile> {
        /* We have to map over the types because the order of the strings is
         * not generally guaranteed, so we can't just copy the underlying
         * structures.
         */
        let sample_types = self.extract_api_sample_types();

        let period = self.period.map(|t| api::Period {
            r#type: api::ValueType {
                r#type: self.get_string(t.1.r#type),
                unit: self.get_string(t.1.unit),
            },
            value: t.0,
        });

        let mut profile = ProfileBuilder::new()
            .sample_types(sample_types)
            .period(period)
            .start_time(start_time)
            .limits(self.limits)
            .build()?;

        std::mem::swap(&mut *self, &mut profile);
        Ok(profile)
    }

    /// Add the endpoint data to the endpoint mappings.
    /// The `endpoint` string will be interned.
    pub fn add_endpoint(
        &mut self,
        local_root_span_id: u64,
        endpoint: Cow<str>,
    ) -> anyhow::Result<()> {
        let interned_endpoint = self.intern(endpoint.as_ref())?;

        self.endpoints
            .mappings
            .insert(local_root_span_id, interned_endpoint);

        Ok(())
    }

    pub fn add_endpoint_count(&mut self, endpoint: Cow<str>, value: i64) {
        // todo: intern strings here
        self.endpoints
            .stats
            .add_endpoint_count(endpoint.into_owned(), value);
    }

    pub fn add_upscaling_rule(
        &mut self,
        offset_values: &[usize],
        label_name: &str,
        label_value: &str,
        upscaling_info: UpscalingInfo,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            offset_values.iter().all(|x| x < &self.sample_types.len()),
            "Invalid offset. Highest expected offset: {}",
            self.sample_types.len() - 1
        );

        let label_name_id = self.intern(label_name)?;
        let label_value_id = self.intern(label_value)?;

        let mut new_values_offset = offset_values.to_vec();
        new_values_offset.sort_unstable();

        self.upscaling_rules.check_collisions(
            &new_values_offset,
            (label_name, label_name_id),
            (label_value, label_value_id),
            &upscaling_info,
        )?;

        upscaling_info.check_validity(self.sample_types.len())?;

        let rule = UpscalingRule::new(new_values_offset, upscaling_info);

        self.upscaling_rules
            .add(label_name_id, label_value_id, rule);

        Ok(())
    }

    /// Serialize the aggregated profile, adding the end time and duration.
    /// # Arguments
    /// * `end_time` - Optional end time of the profile. Passing None will use the current time.
    /// * `duration` - Optional duration of the profile. Passing None will try to calculate the
    ///                duration based on the end time minus the start time, but under anomalous
    ///                conditions this may fail as system clocks can be adjusted. The programmer
    ///                may also accidentally pass an earlier time. The duration will be set to zero
    ///                these cases.
    pub fn serialize(
        &self,
        end_time: Option<SystemTime>,
        duration: Option<Duration>,
    ) -> anyhow::Result<EncodedProfile> {
        let end = end_time.unwrap_or_else(SystemTime::now);
        let start = self.start_time;
        let mut profile: pprof::Profile = self.try_into()?;

        profile.duration_nanos = duration
            .unwrap_or_else(|| {
                end.duration_since(start).unwrap_or({
                    // Let's not throw away the whole profile just because the clocks were wrong.
                    // todo: log that the clock went backward (or programmer mistake).
                    Duration::ZERO
                })
            })
            .as_nanos()
            .min(i64::MAX as u128) as i64;

        // On 2023-08-23, we analyzed the uploaded tarball size per language.
        // These tarballs include 1 or more profiles, but for most languages
        // using libdatadog (all?) there is only 1 profile, so this is a good
        // proxy for the compressed, final size of the profiles.
        // We found that for all languages using libdatadog, the average
        // tarball was at least 18 KiB. Since these archives are compressed,
        // and because profiles compress well, especially ones with timeline
        // enabled (over 9x for some analyzed timeline profiles), this initial
        // size of 32KiB should definitely out-perform starting at zero for
        // time consumed, allocator pressure, and allocator fragmentation.
        const INITIAL_PPROF_BUFFER_SIZE: usize = 32 * 1024;
        let mut buffer: Vec<u8> = Vec::with_capacity(INITIAL_PPROF_BUFFER_SIZE);
        profile.encode(&mut buffer)?;

        Ok(EncodedProfile {
            start,
            end,
            buffer,
            endpoints_stats: self.endpoints.stats.clone(),
        })
    }

    pub fn get_label(&self, id: LabelId) -> &Label {
        self.labels
            .get_index(id.to_offset())
            .expect("LabelId to have a valid interned index")
    }

    pub fn get_label_set(&self, id: LabelSetId) -> &LabelSet {
        self.label_sets
            .get_index(id.to_offset())
            .expect("LabelSetId to have a valid interned index")
    }

    pub fn get_string(&self, id: StringId) -> &str {
        self.strings.get_id(id)
    }

    /// Fetches the endpoint information for the label. There may be errors,
    /// but there may also be no endpoint information for a given endpoint.
    /// Hence, the return type of Result<Option<_>, _>.
    fn get_endpoint_for_label(&self, label: &Label) -> anyhow::Result<Option<Label>> {
        anyhow::ensure!(
            label.get_key() == self.endpoints.local_root_span_id_label,
            "bug: get_endpoint_for_label should only be called on labels with the key \"local root span id\", called on label with key \"{}\"",
            self.get_string(label.get_key())
        );

        anyhow::ensure!(
            label.has_num_value(),
            "the local root span id label value must be sent as a number, not a string, given {:?}",
            label
        );

        let local_root_span_id: u64 = if let LabelValue::Num { num, .. } = label.get_value() {
            // Manually specify the type here to be sure we're transmuting an
            // i64 and not a &i64.
            let id: i64 = *num;
            // Safety: the value is a u64, but pprof only has signed values, so we
            // transmute it; the backend does the same.
            unsafe { std::intrinsics::transmute(id) }
        } else {
            return Err(anyhow::format_err!("the local root span id label value must be sent as a number, not a string, given {:?}",
            label));
        };

        Ok(self
            .endpoints
            .mappings
            .get(&local_root_span_id)
            .map(|v| Label::str(self.endpoints.endpoint_label, *v)))
    }

    fn get_endpoint_for_labels(&self, label_set_id: LabelSetId) -> anyhow::Result<Option<Label>> {
        let label = self.get_label_set(label_set_id).iter().find_map(|id| {
            let label = self.get_label(*id);
            if label.get_key() == self.endpoints.local_root_span_id_label {
                Some(label)
            } else {
                None
            }
        });
        if let Some(label) = label {
            self.get_endpoint_for_label(label)
        } else {
            Ok(None)
        }
    }

    fn translate_and_enrich_sample_labels(
        &self,
        sample: Sample,
        timestamp: Option<Timestamp>,
    ) -> anyhow::Result<Vec<pprof::Label>> {
        let labels: Vec<_> = self
            .get_label_set(sample.labels)
            .iter()
            .map(|l| self.get_label(*l).into())
            .chain(
                self.get_endpoint_for_labels(sample.labels)?
                    .map(pprof::Label::from),
            )
            .chain(timestamp.map(|ts| Label::num(self.timestamp_key, ts.get(), None).into()))
            .collect();

        Ok(labels)
    }
}

impl TryFrom<&Profile> for pprof::Profile {
    type Error = anyhow::Error;

    fn try_from(profile: &Profile) -> anyhow::Result<pprof::Profile> {
        let (period, period_type) = match profile.period {
            Some(tuple) => (tuple.0, Some(tuple.1)),
            None => (0, None),
        };

        /* Rust pattern: inverting Vec<Result<T,E>> into Result<Vec<T>, E> error with .collect:
         * https://doc.rust-lang.org/rust-by-example/error/iter_result.html#fail-the-entire-operation-with-collect
         */

        let samples: anyhow::Result<Vec<pprof::Sample>> = profile
            .observations
            .iter()
            .map(|(sample, timestamp, values)| {
                let labels = profile.translate_and_enrich_sample_labels(sample, timestamp)?;
                let location_ids: Vec<_> = profile
                    .get_stacktrace(sample.stacktrace)
                    .locations
                    .iter()
                    .map(Id::to_raw_id)
                    .collect();
                let values = profile.upscaling_rules.upscale_values(
                    values,
                    &labels,
                    &profile.sample_types,
                )?;

                Ok(pprof::Sample {
                    location_ids,
                    values,
                    labels,
                })
            })
            .collect();
        let samples = samples?;
        Ok(pprof::Profile {
            sample_types: profile
                .sample_types
                .iter()
                .map(pprof::ValueType::from)
                .collect(),
            samples,
            mappings: profile.mappings.to_pprof_vec(),
            locations: profile.locations.to_pprof_vec(),
            functions: profile.functions.to_pprof_vec(),
            string_table: profile.strings.iter().map(String::from).collect(),
            time_nanos: profile
                .start_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |duration| {
                    duration.as_nanos().min(i64::MAX as u128) as i64
                }),
            period,
            period_type: period_type.map(pprof::ValueType::from),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod api_test {

    use super::*;
    use std::num::NonZeroUsize;
    use std::{borrow::Cow, collections::HashMap};

    fn test_builder<'a>() -> ProfileBuilder<'a> {
        const MIB: usize = 1024 * 1024;
        Profile::builder().limits(api::Limits {
            functions_mem: NonZeroUsize::new(MIB).unwrap(),
            locations_mem: NonZeroUsize::new(MIB).unwrap(),
            mappings_mem: NonZeroUsize::new(MIB).unwrap(),
            strings_mem: NonZeroUsize::new(MIB).unwrap(),
        })
    }

    #[test]
    fn test_interning() -> anyhow::Result<()> {
        let sample_types = vec![api::ValueType {
            r#type: "samples",
            unit: "count",
        }];
        let mut profiles = test_builder().sample_types(sample_types).build()?;

        let expected_id = StringId::from_offset(profiles.interned_strings_count());

        let string = "a";
        let id1 = profiles.intern(string)?;
        let id2 = profiles.intern(string)?;

        assert_eq!(id1, id2);
        assert_eq!(id1, expected_id);
        Ok(())
    }

    #[test]
    fn test_api() -> anyhow::Result<()> {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let mapping = api::Mapping {
            filename: "php",
            ..Default::default()
        };

        let index = api::Function {
            filename: "index.php",
            ..Default::default()
        };

        let locations = vec![
            api::Location {
                mapping,
                function: api::Function {
                    name: "phpinfo",
                    system_name: "phpinfo",
                    filename: "index.php",
                    start_line: 0,
                },
                ..Default::default()
            },
            api::Location {
                mapping,
                function: index,
                line: 3,
                ..Default::default()
            },
        ];

        let mut profile = test_builder().sample_types(sample_types).build()?;
        assert_eq!(profile.only_for_testing_num_aggregated_samples(), 0);

        profile.add(api::Sample {
            locations,
            values: vec![1, 10000],
            labels: vec![],
        })?;

        assert_eq!(profile.only_for_testing_num_aggregated_samples(), 1);
        Ok(())
    }

    fn provide_distinct_locations() -> anyhow::Result<Profile> {
        let sample_types = vec![api::ValueType {
            r#type: "samples",
            unit: "count",
        }];

        let mapping = api::Mapping {
            filename: "php",
            ..Default::default()
        };

        let main_locations = vec![api::Location {
            mapping,
            function: api::Function {
                name: "{main}",
                system_name: "{main}",
                filename: "index.php",
                ..Default::default()
            },
            ..Default::default()
        }];
        let test_locations = vec![api::Location {
            mapping,
            function: api::Function {
                name: "test",
                system_name: "test",
                filename: "index.php",
                start_line: 3,
            },
            ..Default::default()
        }];
        let timestamp_locations = vec![api::Location {
            mapping,
            function: api::Function {
                name: "test",
                system_name: "test",
                filename: "index.php",
                start_line: 4,
            },
            ..Default::default()
        }];

        let values: Vec<i64> = vec![1];
        let mut labels = vec![api::Label {
            key: "pid",
            num: 101,
            ..Default::default()
        }];

        let main_sample = api::Sample {
            locations: main_locations,
            values: values.clone(),
            labels: labels.clone(),
        };

        let test_sample = api::Sample {
            locations: test_locations,
            values: values.clone(),
            labels: labels.clone(),
        };

        labels.push(api::Label {
            key: "end_timestamp_ns",
            num: 42,
            ..Default::default()
        });
        let timestamp_sample = api::Sample {
            locations: timestamp_locations,
            values,
            labels,
        };

        let mut profile = test_builder().sample_types(sample_types).build()?;
        assert_eq!(profile.only_for_testing_num_aggregated_samples(), 0);

        profile.add(main_sample).expect("profile to not be full");
        assert_eq!(profile.only_for_testing_num_aggregated_samples(), 1);

        profile.add(test_sample).expect("profile to not be full");
        assert_eq!(profile.only_for_testing_num_aggregated_samples(), 2);

        assert_eq!(profile.only_for_testing_num_timestamped_samples(), 0);
        profile
            .add(timestamp_sample)
            .expect("profile to not be full");
        assert_eq!(profile.only_for_testing_num_timestamped_samples(), 1);
        Ok(profile)
    }

    #[test]
    fn test_impl_from_profile_for_pprof_profile() -> anyhow::Result<()> {
        let locations = provide_distinct_locations()?;
        let profile = pprof::Profile::try_from(&locations)?;

        assert_eq!(profile.samples.len(), 3);
        assert_eq!(profile.mappings.len(), 1);
        assert_eq!(profile.locations.len(), 3);
        assert_eq!(profile.functions.len(), 3);

        for (index, mapping) in profile.mappings.iter().enumerate() {
            assert_eq!((index + 1) as u64, mapping.id);
        }

        for (index, location) in profile.locations.iter().enumerate() {
            assert_eq!((index + 1) as u64, location.id);
        }

        for (index, function) in profile.functions.iter().enumerate() {
            assert_eq!((index + 1) as u64, function.id);
        }
        let samples = profile.sorted_samples();

        let sample = samples.get(0).expect("index 0 to exist");
        assert_eq!(sample.labels.len(), 1);
        let label = sample.labels.get(0).expect("index 0 to exist");
        let key = profile
            .string_table
            .get(label.key as usize)
            .expect("index to exist");
        let str = profile
            .string_table
            .get(label.str as usize)
            .expect("index to exist");
        let num_unit = profile
            .string_table
            .get(label.num_unit as usize)
            .expect("index to exist");
        assert_eq!(key, "pid");
        assert_eq!(label.num, 101);
        assert_eq!(str, "");
        assert_eq!(num_unit, "");

        let sample = samples.get(1).expect("index 1 to exist");
        assert_eq!(sample.labels.len(), 1);
        let label = sample.labels.get(0).expect("index 0 to exist");
        let key = profile
            .string_table
            .get(label.key as usize)
            .expect("index to exist");
        let str = profile
            .string_table
            .get(label.str as usize)
            .expect("index to exist");
        let num_unit = profile
            .string_table
            .get(label.num_unit as usize)
            .expect("index to exist");
        assert_eq!(key, "pid");
        assert_eq!(label.num, 101);
        assert_eq!(str, "");
        assert_eq!(num_unit, "");

        let sample = samples.get(2).expect("index 2 to exist");
        assert_eq!(sample.labels.len(), 2);
        let label = sample.labels.get(0).expect("index 0 to exist");
        let key = profile
            .string_table
            .get(label.key as usize)
            .expect("index to exist");
        let str = profile
            .string_table
            .get(label.str as usize)
            .expect("index to exist");
        let num_unit = profile
            .string_table
            .get(label.num_unit as usize)
            .expect("index to exist");
        assert_eq!(key, "pid");
        assert_eq!(label.num, 101);
        assert_eq!(str, "");
        assert_eq!(num_unit, "");
        let label = sample.labels.get(1).expect("index 1 to exist");
        let key = profile
            .string_table
            .get(label.key as usize)
            .expect("index to exist");
        let str = profile
            .string_table
            .get(label.str as usize)
            .expect("index to exist");
        let num_unit = profile
            .string_table
            .get(label.num_unit as usize)
            .expect("index to exist");
        assert_eq!(key, "end_timestamp_ns");
        assert_eq!(label.num, 42);
        assert_eq!(str, "");
        assert_eq!(num_unit, "");
        Ok(())
    }

    #[test]
    fn test_reset() -> anyhow::Result<()> {
        let mut profile = provide_distinct_locations()?;
        /* This set of asserts is to make sure it's a non-empty profile that we
         * are working with so that we can test that reset works.
         */
        assert!(!profile.functions.is_empty());
        assert!(!profile.labels.is_empty());
        assert!(!profile.label_sets.is_empty());
        assert!(!profile.locations.is_empty());
        assert!(!profile.mappings.is_empty());
        assert!(!profile.observations.is_empty());
        assert!(!profile.sample_types.is_empty());
        assert!(profile.period.is_none());
        assert!(profile.endpoints.mappings.is_empty());
        assert!(profile.endpoints.stats.is_empty());

        let prev = profile.reset(None)?;

        // These should all be empty now
        assert!(profile.functions.is_empty());
        assert!(profile.labels.is_empty());
        assert!(profile.label_sets.is_empty());
        assert!(profile.locations.is_empty());
        assert!(profile.mappings.is_empty());
        assert!(profile.observations.is_empty());
        assert!(profile.endpoints.mappings.is_empty());
        assert!(profile.endpoints.stats.is_empty());
        assert!(profile.upscaling_rules.is_empty());

        assert_eq!(profile.period, prev.period);
        assert_eq!(profile.sample_types, prev.sample_types);

        // The string table should have at least the empty string.
        assert!(!profile.strings.is_empty());
        assert_eq!("", profile.get_string(StringId::ZERO));
        Ok(())
    }

    #[test]
    fn test_reset_period() -> anyhow::Result<()> {
        /* The previous test (reset) checked quite a few properties already, so
         * this one will focus only on the period.
         */
        let mut profile = provide_distinct_locations()?;

        let period = (
            10_000_000,
            ValueType {
                r#type: profile.intern("wall-time")?,
                unit: profile.intern("nanoseconds")?,
            },
        );
        profile.period = Some(period);

        let prev = profile.reset(None)?;
        assert_eq!(Some(period), prev.period);

        // Resolve the string values to check that they match (their string
        // table offsets may not match).
        let (value, period_type) = profile.period.expect("profile to have a period");
        assert_eq!(value, period.0);
        assert_eq!(profile.get_string(period_type.r#type), "wall-time");
        assert_eq!(profile.get_string(period_type.unit), "nanoseconds");
        Ok(())
    }

    #[test]
    fn test_adding_local_root_span_id_with_string_value_fails() -> anyhow::Result<()> {
        let sample_types = vec![api::ValueType {
            r#type: "wall-time",
            unit: "nanoseconds",
        }];

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = api::Label {
            key: "local root span id",
            str: Some("10"), // bad value, should use .num instead for local root span id
            num: 0,
            num_unit: None,
        };

        let sample = api::Sample {
            locations: vec![],
            values: vec![1, 10000],
            labels: vec![id_label],
        };

        assert!(profile.add(sample).is_err());
        Ok(())
    }

    #[test]
    fn test_lazy_endpoints() -> anyhow::Result<()> {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = api::Label {
            key: "local root span id",
            str: None,
            num: 10,
            num_unit: None,
        };

        let id2_label = api::Label {
            key: "local root span id",
            str: None,
            num: 11,
            num_unit: None,
        };

        let other_label = api::Label {
            key: "other",
            str: Some("test"),
            num: 0,
            num_unit: None,
        };

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000],
            labels: vec![id_label, other_label],
        };

        let sample2 = api::Sample {
            locations: vec![],
            values: vec![1, 10000],
            labels: vec![id2_label, other_label],
        };

        profile.add(sample1)?;

        profile.add(sample2)?;

        profile.add_endpoint(10, Cow::from("my endpoint"))?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;
        assert_eq!(serialized_profile.samples.len(), 2);
        let samples = serialized_profile.sorted_samples();

        let s1 = samples.get(0).expect("sample");

        // The trace endpoint label should be added to the first sample
        assert_eq!(s1.labels.len(), 3);

        let l1 = s1.labels.get(0).expect("label");

        assert_eq!(
            serialized_profile
                .string_table
                .get(l1.key as usize)
                .unwrap(),
            "local root span id"
        );
        assert_eq!(l1.num, 10);

        let l2 = s1.labels.get(1).expect("label");

        assert_eq!(
            serialized_profile
                .string_table
                .get(l2.key as usize)
                .unwrap(),
            "other"
        );
        assert_eq!(
            serialized_profile
                .string_table
                .get(l2.str as usize)
                .unwrap(),
            "test"
        );

        let l3 = s1.labels.get(2).expect("label");

        assert_eq!(
            serialized_profile
                .string_table
                .get(l3.key as usize)
                .unwrap(),
            "trace endpoint"
        );
        assert_eq!(
            serialized_profile
                .string_table
                .get(l3.str as usize)
                .unwrap(),
            "my endpoint"
        );

        let s2 = samples.get(1).expect("sample");

        // The trace endpoint label shouldn't be added to second sample because the span id doesn't match
        assert_eq!(s2.labels.len(), 2);
        Ok(())
    }

    #[test]
    fn test_endpoint_counts_empty_test() -> anyhow::Result<()> {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let profile: Profile = test_builder().sample_types(sample_types).build()?;

        let encoded_profile = profile
            .serialize(None, None)
            .expect("Unable to encode/serialize the profile");

        let endpoints_stats = encoded_profile.endpoints_stats;
        assert!(endpoints_stats.is_empty());
        Ok(())
    }

    #[test]
    fn test_endpoint_counts_test() -> anyhow::Result<()> {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let one_endpoint = "my endpoint";
        profile.add_endpoint_count(Cow::from(one_endpoint), 1);
        profile.add_endpoint_count(Cow::from(one_endpoint), 1);

        let second_endpoint = "other endpoint";
        profile.add_endpoint_count(Cow::from(second_endpoint), 1);

        let encoded_profile = profile.serialize(None, None)?;

        let endpoints_stats = encoded_profile.endpoints_stats;

        let mut count: HashMap<String, i64> = HashMap::new();
        count.insert(one_endpoint.to_string(), 2);
        count.insert(second_endpoint.to_string(), 1);

        let expected_endpoints_stats = ProfiledEndpointsStats::from(count);

        assert_eq!(endpoints_stats, expected_endpoints_stats);
        Ok(())
    }

    #[test]
    fn local_root_span_id_label_cannot_occur_more_than_once() -> anyhow::Result<()> {
        let sample_types = vec![api::ValueType {
            r#type: "wall-time",
            unit: "nanoseconds",
        }];

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let labels = vec![
            api::Label {
                key: "local root span id",
                str: None,
                num: 5738080760940355267_i64,
                num_unit: None,
            },
            api::Label {
                key: "local root span id",
                str: None,
                num: 8182855815056056749_i64,
                num_unit: None,
            },
        ];

        let sample = api::Sample {
            locations: vec![],
            values: vec![10000],
            labels,
        };

        profile.add(sample).unwrap_err();
        Ok(())
    }

    #[test]
    fn test_no_upscaling_if_no_rules() -> anyhow::Result<()> {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = api::Label {
            key: "my label",
            str: Some("coco"),
            num: 0,
            num_unit: None,
        };

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000],
            labels: vec![id_label],
        };

        profile.add(sample1)?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values[0], 1);
        assert_eq!(first.values[1], 10000);
        Ok(())
    }

    fn create_samples_types() -> Vec<api::ValueType<'static>> {
        vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
            api::ValueType {
                r#type: "cpu-time",
                unit: "nanoseconds",
            },
        ]
    }

    fn create_label(key: &'static str, str: Option<&'static str>) -> api::Label<'static> {
        api::Label {
            key,
            str,
            num: 0,
            num_unit: None,
        }
    }

    #[test]
    fn test_upscaling_by_value_a_zero_value() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile = test_builder().sample_types(sample_types).build()?;

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![0, 10000, 42],
            labels: vec![],
        };

        profile.add(sample1)?;

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        let values_offset = vec![0];
        profile.add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info)?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![0, 10000, 42]);
        Ok(())
    }

    #[test]
    fn test_upscaling_by_value_on_one_value() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 42],
            labels: vec![],
        };

        profile.add(sample1)?;

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.7 };
        let values_offset = vec![0];
        profile.add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info)?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![3, 10000, 42]);
        Ok(())
    }

    #[test]
    fn test_upscaling_by_value_on_one_value_with_poisson() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile = test_builder().sample_types(sample_types).build()?;

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 16, 29],
            labels: vec![],
        };

        profile.add(sample1)?;

        let upscaling_info = UpscalingInfo::Poisson {
            sum_value_offset: 1,
            count_value_offset: 2,
            sampling_distance: 10,
        };
        let values_offset: Vec<usize> = vec![1];
        profile.add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info)?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![1, 298, 29]);
        Ok(())
    }

    #[test]
    fn test_upscaling_by_value_on_zero_value_with_poisson() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile = test_builder().sample_types(sample_types).build()?;

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 16, 0],
            labels: vec![],
        };

        profile.add(sample1)?;

        let upscaling_info = UpscalingInfo::Poisson {
            sum_value_offset: 1,
            count_value_offset: 2,
            sampling_distance: 10,
        };
        let values_offset: Vec<usize> = vec![1];
        profile.add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info)?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![1, 16, 0]);
        Ok(())
    }

    #[test]
    fn test_cannot_add_a_rule_with_invalid_poisson_info() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 16, 0],
            labels: vec![],
        };

        profile.add(sample1)?;

        // invalid sampling_distance vaue
        let upscaling_info = UpscalingInfo::Poisson {
            sum_value_offset: 1,
            count_value_offset: 2,
            sampling_distance: 0,
        };

        let values_offset: Vec<usize> = vec![1];
        profile
            .add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info)
            .expect_err("Cannot add a rule if sampling_distance is equal to 0");

        // x value is greater than the number of value types
        let upscaling_info2 = UpscalingInfo::Poisson {
            sum_value_offset: 42,
            count_value_offset: 2,
            sampling_distance: 10,
        };
        profile
            .add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info2)
            .expect_err("Cannot add a rule if the offset x is invalid");

        // y value is greater than the number of value types
        let upscaling_info3 = UpscalingInfo::Poisson {
            sum_value_offset: 1,
            count_value_offset: 42,
            sampling_distance: 10,
        };
        profile
            .add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info3)
            .expect_err("Cannot add a rule if the offset y is invalid");

        Ok(())
    }

    #[test]
    fn test_upscaling_by_value_on_two_values() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 21],
            labels: vec![],
        };

        let mapping = api::Mapping {
            filename: "php",
            ..Default::default()
        };

        let main_locations = vec![api::Location {
            mapping,
            function: api::Function {
                name: "{main}",
                system_name: "{main}",
                filename: "index.php",
                start_line: 0,
            },
            address: 0,
            line: 0,
        }];

        let sample2 = api::Sample {
            locations: main_locations,
            values: vec![5, 24, 99],
            labels: vec![],
        };

        profile.add(sample1)?;
        profile.add(sample2)?;

        // upscale the first value and the last one
        let values_offset: Vec<usize> = vec![0, 2];

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        profile.add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info)?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;
        let samples = serialized_profile.sorted_samples();
        let first = samples.get(0).expect("first sample");

        assert_eq!(first.values, vec![2, 10000, 42]);

        let second = samples.get(1).expect("second sample");

        assert_eq!(second.values, vec![10, 24, 198]);
        Ok(())
    }

    #[test]
    fn test_upscaling_by_value_on_two_value_with_two_rules() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 21],
            labels: vec![],
        };

        let mapping = api::Mapping {
            filename: "php",
            ..Default::default()
        };

        let main_locations = vec![api::Location {
            mapping,
            function: api::Function {
                name: "{main}",
                system_name: "{main}",
                filename: "index.php",
                start_line: 0,
            },
            ..Default::default()
        }];

        let sample2 = api::Sample {
            locations: main_locations,
            values: vec![5, 24, 99],
            labels: vec![],
        };

        profile.add(sample1)?;
        profile.add(sample2)?;

        let mut values_offset: Vec<usize> = vec![0];

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        profile.add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info)?;

        // add another byvaluerule on the 3rd offset
        values_offset.clear();
        values_offset.push(2);

        let upscaling_info2 = UpscalingInfo::Proportional { scale: 5.0 };

        profile.add_upscaling_rule(values_offset.as_slice(), "", "", upscaling_info2)?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;
        let samples = serialized_profile.sorted_samples();
        let first = samples.get(0).expect("first sample");

        assert_eq!(first.values, vec![2, 10000, 105]);

        let second = samples.get(1).expect("second sample");

        assert_eq!(second.values, vec![10, 24, 495]);
        Ok(())
    }
    #[test]
    fn test_no_upscaling_by_label_if_no_match() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = create_label("my_label", Some("coco"));

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 42],
            labels: vec![id_label],
        };

        profile.add(sample1)?;

        let values_offset: Vec<usize> = vec![0];

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        profile.add_upscaling_rule(
            values_offset.as_slice(),
            "my label",
            "foobar",
            upscaling_info,
        )?;

        let upscaling_info2 = UpscalingInfo::Proportional { scale: 2.0 };
        profile.add_upscaling_rule(
            values_offset.as_slice(),
            "my other label",
            "coco",
            upscaling_info2,
        )?;

        let upscaling_info3 = UpscalingInfo::Proportional { scale: 2.0 };
        profile.add_upscaling_rule(
            values_offset.as_slice(),
            "my other label",
            "foobar",
            upscaling_info3,
        )?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![1, 10000, 42]);
        Ok(())
    }

    #[test]
    fn test_upscaling_by_label_on_one_value() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = create_label("my label", Some("coco"));

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 42],
            labels: vec![id_label],
        };

        profile.add(sample1)?;

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        let values_offset: Vec<usize> = vec![0];
        profile.add_upscaling_rule(
            values_offset.as_slice(),
            id_label.key,
            id_label.str.unwrap(),
            upscaling_info,
        )?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![2, 10000, 42]);
        Ok(())
    }

    #[test]
    fn test_upscaling_by_label_on_only_sample_out_of_two() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = create_label("my label", Some("coco"));

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 42],
            labels: vec![id_label],
        };

        let mapping = api::Mapping {
            filename: "php",
            ..Default::default()
        };

        let main_locations = vec![api::Location {
            mapping,
            function: api::Function {
                name: "{main}",
                system_name: "{main}",
                filename: "index.php",
                start_line: 0,
            },
            ..Default::default()
        }];

        let sample2 = api::Sample {
            locations: main_locations,
            values: vec![5, 24, 99],
            labels: vec![],
        };

        profile.add(sample1)?;
        profile.add(sample2)?;

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        let values_offset: Vec<usize> = vec![0];
        profile.add_upscaling_rule(
            values_offset.as_slice(),
            id_label.key,
            id_label.str.unwrap(),
            upscaling_info,
        )?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;
        let samples = serialized_profile.sorted_samples();

        let first = samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![2, 10000, 42]);

        let second = samples.get(1).expect("one sample");

        assert_eq!(second.values, vec![5, 24, 99]);
        Ok(())
    }

    #[test]
    fn test_upscaling_by_label_with_two_different_rules_on_two_different_sample(
    ) -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_no_match_label = create_label("another label", Some("do not care"));

        let id_label = create_label("my label", Some("coco"));

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 42],
            labels: vec![id_label, id_no_match_label],
        };

        let mapping = api::Mapping {
            filename: "php",
            ..Default::default()
        };

        let main_locations = vec![api::Location {
            mapping,
            function: api::Function {
                name: "{main}",
                system_name: "{main}",
                filename: "index.php",
                start_line: 0,
            },
            ..Default::default()
        }];

        let id_label2 = api::Label {
            key: "my other label",
            str: Some("foobar"),
            num: 10,
            num_unit: None,
        };

        let sample2 = api::Sample {
            locations: main_locations,
            values: vec![5, 24, 99],
            labels: vec![id_no_match_label, id_label2],
        };

        profile.add(sample1)?;
        profile.add(sample2)?;

        // add rule for the first sample on the 1st value
        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        let mut values_offset: Vec<usize> = vec![0];
        profile.add_upscaling_rule(
            values_offset.as_slice(),
            id_label.key,
            id_label.str.unwrap(),
            upscaling_info,
        )?;

        // add rule for the second sample on the 3rd value
        let upscaling_info2 = UpscalingInfo::Proportional { scale: 10.0 };
        values_offset.clear();
        values_offset.push(2);
        profile.add_upscaling_rule(
            values_offset.as_slice(),
            id_label2.key,
            id_label2.str.unwrap(),
            upscaling_info2,
        )?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;
        let samples = serialized_profile.sorted_samples();
        let first = samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![2, 10000, 42]);

        let second = samples.get(1).expect("one sample");

        assert_eq!(second.values, vec![5, 24, 990]);
        Ok(())
    }

    #[test]
    fn test_upscaling_by_label_on_two_values() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = create_label("my label", Some("coco"));

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 42],
            labels: vec![id_label],
        };

        profile.add(sample1)?;

        // upscale samples and wall-time values
        let values_offset: Vec<usize> = vec![0, 1];

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        profile.add_upscaling_rule(
            values_offset.as_slice(),
            id_label.key,
            id_label.str.unwrap(),
            upscaling_info,
        )?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![2, 20000, 42]);
        Ok(())
    }
    #[test]
    fn test_upscaling_by_value_and_by_label_different_values() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = create_label("my label", Some("coco"));

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 42],
            labels: vec![id_label],
        };

        profile.add(sample1)?;

        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        let mut value_offsets: Vec<usize> = vec![0];
        profile.add_upscaling_rule(value_offsets.as_slice(), "", "", upscaling_info)?;

        // a bylabel rule on the third offset
        let upscaling_info2 = UpscalingInfo::Proportional { scale: 5.0 };
        value_offsets.clear();
        value_offsets.push(2);
        profile.add_upscaling_rule(
            value_offsets.as_slice(),
            id_label.key,
            id_label.str.unwrap(),
            upscaling_info2,
        )?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;

        assert_eq!(serialized_profile.samples.len(), 1);
        let first = serialized_profile.samples.get(0).expect("one sample");

        assert_eq!(first.values, vec![2, 10000, 210]);
        Ok(())
    }

    #[test]
    fn test_add_same_byvalue_rule_twice() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        // adding same offsets
        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        let mut value_offsets: Vec<usize> = vec![0, 2];
        profile.add_upscaling_rule(value_offsets.as_slice(), "", "", upscaling_info)?;

        let upscaling_info2 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(value_offsets.as_slice(), "", "", upscaling_info2)
            .expect_err("Duplicated rules");

        // adding offsets with overlap on 2
        value_offsets.clear();
        value_offsets.push(2);
        value_offsets.push(1);
        let upscaling_info3 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(value_offsets.as_slice(), "", "", upscaling_info3)
            .expect_err("Duplicated rules");

        // same offsets in different order
        value_offsets.clear();
        value_offsets.push(2);
        value_offsets.push(0);
        let upscaling_info4 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(value_offsets.as_slice(), "", "", upscaling_info4)
            .expect_err("Duplicated rules");
        Ok(())
    }

    #[test]
    fn test_add_two_bylabel_rules_with_overlap_on_values() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        // adding same offsets
        let mut value_offsets: Vec<usize> = vec![0, 2];
        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        profile.add_upscaling_rule(value_offsets.as_slice(), "my label", "coco", upscaling_info)?;
        let upscaling_info2 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(
                value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info2,
            )
            .expect_err("Duplicated rules");

        // adding offsets with overlap on 2
        value_offsets.clear();
        value_offsets.append(&mut vec![2, 1]);
        let upscaling_info3 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(
                value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info3,
            )
            .expect_err("Duplicated rules");

        // same offsets in different order
        value_offsets.clear();
        value_offsets.push(2);
        value_offsets.push(0);
        let upscaling_info4 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(
                value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info4,
            )
            .expect_err("Duplicated rules");
        Ok(())
    }

    #[test]
    fn test_fail_if_bylabel_rule_and_by_value_rule_with_overlap_on_values() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        // adding same offsets
        let mut value_offsets: Vec<usize> = vec![0, 2];
        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };

        // add by value rule
        profile.add_upscaling_rule(value_offsets.as_slice(), "", "", upscaling_info)?;

        // add by-label rule
        let upscaling_info2 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(
                value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info2,
            )
            .expect_err("Duplicated rules");

        // adding offsets with overlap on 2
        value_offsets.clear();
        value_offsets.append(&mut vec![2, 1]);
        let upscaling_info3 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(
                value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info3,
            )
            .expect_err("Duplicated rules");

        // same offsets in different order
        value_offsets.clear();
        value_offsets.push(2);
        value_offsets.push(0);
        let upscaling_info4 = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(
                value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info4,
            )
            .expect_err("Duplicated rules");
        Ok(())
    }

    #[test]
    fn test_add_rule_with_offset_out_of_bound() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        // adding same offsets
        let by_value_offsets: Vec<usize> = vec![0, 4];
        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        profile
            .add_upscaling_rule(
                by_value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info,
            )
            .expect_err("Invalid offset");
        Ok(())
    }

    #[test]
    fn test_add_rule_with_offset_out_of_bound_poisson_function() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        // adding same offsets
        let by_value_offsets: Vec<usize> = vec![0, 4];
        let upscaling_info = UpscalingInfo::Poisson {
            sum_value_offset: 1,
            count_value_offset: 100,
            sampling_distance: 1,
        };
        profile
            .add_upscaling_rule(
                by_value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info,
            )
            .expect_err("Invalid offset");
        Ok(())
    }

    #[test]
    fn test_add_rule_with_offset_out_of_bound_poisson_function2() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        // adding same offsets
        let by_value_offsets: Vec<usize> = vec![0, 4];
        let upscaling_info = UpscalingInfo::Poisson {
            sum_value_offset: 100,
            count_value_offset: 1,
            sampling_distance: 1,
        };
        profile
            .add_upscaling_rule(
                by_value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info,
            )
            .expect_err("Invalid offset");
        Ok(())
    }

    #[test]
    fn test_add_rule_with_offset_out_of_bound_poisson_function3() -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        // adding same offsets
        let by_value_offsets: Vec<usize> = vec![0, 4];
        let upscaling_info = UpscalingInfo::Poisson {
            sum_value_offset: 1100,
            count_value_offset: 100,
            sampling_distance: 1,
        };
        profile
            .add_upscaling_rule(
                by_value_offsets.as_slice(),
                "my label",
                "coco",
                upscaling_info,
            )
            .expect_err("Invalid offset");
        Ok(())
    }

    #[test]
    fn test_fails_when_adding_byvalue_rule_collinding_on_offset_with_existing_bylabel_rule(
    ) -> anyhow::Result<()> {
        let sample_types = create_samples_types();

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = create_label("my label", Some("coco"));

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000, 42],
            labels: vec![id_label],
        };

        profile.add(sample1)?;

        let mut value_offsets: Vec<usize> = vec![0, 1];
        // Add by-label rule first
        let upscaling_info2 = UpscalingInfo::Proportional { scale: 2.0 };
        profile.add_upscaling_rule(
            value_offsets.as_slice(),
            id_label.key,
            id_label.str.unwrap(),
            upscaling_info2,
        )?;

        // add by-value rule
        let upscaling_info = UpscalingInfo::Proportional { scale: 2.0 };
        value_offsets.clear();
        value_offsets.push(0);
        profile
            .add_upscaling_rule(value_offsets.as_slice(), "", "", upscaling_info)
            .expect_err("Rule added");
        Ok(())
    }

    #[test]
    fn local_root_span_id_label_as_i64() -> anyhow::Result<()> {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let mut profile: Profile = test_builder().sample_types(sample_types).build()?;

        let id_label = api::Label {
            key: "local root span id",
            str: None,
            num: 10,
            num_unit: None,
        };

        let large_span_id = u64::MAX;
        // Safety: a u64 can fit into an i64, and we're testing that it's not mis-handled.
        let large_num: i64 = unsafe { std::intrinsics::transmute(large_span_id) };

        let id2_label = api::Label {
            key: "local root span id",
            str: None,
            num: large_num,
            num_unit: None,
        };

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000],
            labels: vec![id_label],
        };

        let sample2 = api::Sample {
            locations: vec![],
            values: vec![1, 10000],
            labels: vec![id2_label],
        };

        profile.add(sample1)?;
        profile.add(sample2)?;

        profile.add_endpoint(10, Cow::from("endpoint 10"))?;
        profile.add_endpoint(large_span_id, Cow::from("large endpoint"))?;

        let serialized_profile = pprof::Profile::try_from(&profile)?;
        assert_eq!(serialized_profile.samples.len(), 2);

        // Find common label strings in the string table.
        let locate_string = |string: &str| -> i64 {
            // The table is supposed to be unique, so we shouldn't have to worry about duplicates.
            serialized_profile
                .string_table
                .iter()
                .enumerate()
                .find_map(|(offset, str)| {
                    if str == string {
                        Some(offset as i64)
                    } else {
                        None
                    }
                })
                .unwrap()
        };

        let local_root_span_id = locate_string("local root span id");
        let trace_endpoint = locate_string("trace endpoint");

        // Set up the expected labels per sample
        let expected_labels = [
            [
                pprof::Label {
                    key: local_root_span_id,
                    str: 0,
                    num: large_num,
                    num_unit: 0,
                },
                pprof::Label::str(trace_endpoint, locate_string("large endpoint")),
            ],
            [
                pprof::Label {
                    key: local_root_span_id,
                    str: 0,
                    num: 10,
                    num_unit: 0,
                },
                pprof::Label::str(trace_endpoint, locate_string("endpoint 10")),
            ],
        ];

        // Finally, match the labels.
        for (sample, labels) in serialized_profile
            .sorted_samples()
            .iter()
            .zip(expected_labels.iter())
        {
            assert_eq!(sample.labels, labels);
        }
        Ok(())
    }
}
