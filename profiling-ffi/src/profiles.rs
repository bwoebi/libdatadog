// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

use crate::Timespec;
use datadog_profiling::profile::{self, api, profiled_endpoints};
use ddcommon_ffi::slice::{AsBytes, CharSlice, Slice};
use ddcommon_ffi::Error;
use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;
use std::num::{NonZeroU32, NonZeroUsize, TryFromIntError};
use std::str::Utf8Error;
use std::time::{Duration, SystemTime};

#[repr(C)]
pub struct Profile {
    opaque: usize,
    _marker: PhantomData<Box<profile::Profile>>,
}

impl Profile {
    #[cfg(test)]
    unsafe fn borrow_inner(&self) -> &profile::Profile {
        &*(self.opaque as *const profile::Profile)
    }

    unsafe fn borrow_inner_mut(&mut self) -> &mut profile::Profile {
        &mut *(self.opaque as *mut profile::Profile)
    }
}

impl From<profile::Profile> for Profile {
    fn from(profile: profile::Profile) -> Self {
        Self {
            opaque: Box::into_raw(Box::new(profile)) as usize,
            _marker: PhantomData,
        }
    }
}

impl Drop for Profile {
    fn drop(&mut self) {
        // SAFETY: safe as long as the opaqueness is respected by C, and other
        // requirements like not dropping twice are upheld.
        unsafe {
            let ptr = self.opaque as *mut profile::Profile;
            if !ptr.is_null() {
                drop(Box::from_raw(ptr))
            }
        }
        self.opaque = std::ptr::null_mut::<profile::Profile>() as usize;
    }
}

/// Represents the memory limits of certain structures in the Profile. This
/// does not account for all memory allocated, just stuff in the arenas. The
/// amounts are in bytes, and must be non-zero.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct ProfileLimits {
    pub functions_mem: u32,
    pub locations_mem: u32,
    pub mappings_mem: u32,
    pub strings_mem: u32,
}

fn u32_to_non_zero_usize(num: u32) -> Result<NonZeroUsize, TryFromIntError> {
    let small = NonZeroU32::try_from(num)?;
    NonZeroUsize::try_from(small)
}
impl TryFrom<ProfileLimits> for api::Limits {
    type Error = TryFromIntError;

    fn try_from(value: ProfileLimits) -> Result<Self, Self::Error> {
        Ok(Self {
            functions_mem: u32_to_non_zero_usize(value.functions_mem)?,
            locations_mem: u32_to_non_zero_usize(value.locations_mem)?,
            mappings_mem: u32_to_non_zero_usize(value.mappings_mem)?,
            strings_mem: u32_to_non_zero_usize(value.strings_mem)?,
        })
    }
}

#[repr(C)]
pub enum ProfileNewResult {
    Ok(
        /// Free with `ddog_prof_Profile_drop`.
        Profile,
    ),
    Err(
        /// Free with `ddog_Error_drop`.
        Error,
    ),
}

/// Used for operations which may fail, but don't return anything meaningful
/// on success.
#[repr(C)]
pub enum ProfileResult {
    Ok(
        /// Do not use the value of ok. This value only exists to overcome
        /// Rust -> C code generation.
        bool,
    ),
    Err(
        /// Free with `ddog_Error_drop`.
        Error,
    ),
}

trait IntoProfileResult {
    fn into_profile_result<C>(self, context: C) -> ProfileResult
    where
        C: std::fmt::Display + Send + Sync + 'static;
}

impl IntoProfileResult for anyhow::Result<()> {
    fn into_profile_result<C>(self, context: C) -> ProfileResult
    where
        C: std::fmt::Display + Send + Sync + 'static,
    {
        match self {
            Ok(_) => ProfileResult::Ok(true),
            Err(err) => ProfileResult::Err(Error::from(err.context(context))),
        }
    }
}

#[repr(C)]
pub enum SerializeResult {
    Ok(
        /// Free with `ddog_prof_EncodedProfile_drop`.
        EncodedProfile,
    ),
    Err(
        /// Free with `ddog_Error_drop`.
        Error,
    ),
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ValueType<'a> {
    pub type_: CharSlice<'a>,
    pub unit: CharSlice<'a>,
}

impl<'a> ValueType<'a> {
    pub fn new(type_: &'a str, unit: &'a str) -> Self {
        Self {
            type_: type_.into(),
            unit: unit.into(),
        }
    }
}

#[repr(C)]
pub struct Period<'a> {
    pub type_: ValueType<'a>,
    pub value: i64,
}

#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Label<'a> {
    pub key: CharSlice<'a>,

    /// At most one of the following must be present
    pub str: CharSlice<'a>,
    pub num: i64,

    /// Should only be present when num is present.
    /// Specifies the units of num.
    /// Use arbitrary string (for example, "requests") as a custom count unit.
    /// If no unit is specified, consumer may apply heuristic to deduce the unit.
    /// Consumers may also  interpret units like "bytes" and "kilobytes" as memory
    /// units and units like "seconds" and "nanoseconds" as time units,
    /// and apply appropriate unit conversions to these.
    pub num_unit: CharSlice<'a>,
}

#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Function<'a> {
    /// Name of the function, in human-readable form if available.
    pub name: CharSlice<'a>,

    /// Name of the function, as identified by the system.
    /// For instance, it can be a C++ mangled name.
    pub system_name: CharSlice<'a>,

    /// Source file containing the function.
    pub filename: CharSlice<'a>,

    /// Line number in source file.
    pub start_line: i64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Line<'a> {
    /// The corresponding profile.Function for this line.
    pub function: Function<'a>,

    /// Line number in source code.
    pub line: i64,
}

#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Location<'a> {
    /// todo: how to handle unknown mapping?
    pub mapping: Mapping<'a>,
    pub function: Function<'a>,

    /// The instruction address for this location, if available.  It
    /// should be within [Mapping.memory_start...Mapping.memory_limit]
    /// for the corresponding mapping. A non-leaf address may be in the
    /// middle of a call instruction. It is up to display tools to find
    /// the beginning of the instruction if necessary.
    pub address: u64,
    pub line: i64,
}

#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Mapping<'a> {
    /// Address at which the binary (or DLL) is loaded into memory.
    pub memory_start: u64,

    /// The limit of the address range occupied by this mapping.
    pub memory_limit: u64,

    /// Offset in the binary that corresponds to the first mapped address.
    pub file_offset: u64,

    /// The object this entry is loaded from.  This can be a filename on
    /// disk for the main binary and shared libraries, or virtual
    /// abstractions like "[vdso]".
    pub filename: CharSlice<'a>,

    /// A string that uniquely identifies a particular program version
    /// with high probability. E.g., for binaries generated by GNU tools,
    /// it could be the contents of the .note.gnu.build-id field.
    pub build_id: CharSlice<'a>,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Sample<'a> {
    /// The leaf is at locations[0].
    pub locations: Slice<'a, Location<'a>>,

    /// The type and unit of each value is defined by the corresponding
    /// entry in Profile.sample_type. All samples must have the same
    /// number of values, the same as the length of Profile.sample_type.
    /// When aggregating multiple samples into a single sample, the
    /// result has a list of values that is the element-wise sum of the
    /// lists of the originals.
    pub values: Slice<'a, i64>,

    /// label includes additional context for this sample. It can include
    /// things like a thread id, allocation size, etc
    pub labels: Slice<'a, Label<'a>>,
}

impl<'a> TryFrom<&'a Mapping<'a>> for api::Mapping<'a> {
    type Error = Utf8Error;

    fn try_from(mapping: &'a Mapping<'a>) -> Result<Self, Self::Error> {
        let filename = unsafe { mapping.filename.try_to_utf8() }?;
        let build_id = unsafe { mapping.build_id.try_to_utf8() }?;
        Ok(Self {
            memory_start: mapping.memory_start,
            memory_limit: mapping.memory_limit,
            file_offset: mapping.file_offset,
            filename,
            build_id,
        })
    }
}

impl<'a> From<&'a ValueType<'a>> for api::ValueType<'a> {
    fn from(vt: &'a ValueType<'a>) -> Self {
        unsafe {
            Self {
                r#type: vt.type_.try_to_utf8().unwrap_or(""),
                unit: vt.unit.try_to_utf8().unwrap_or(""),
            }
        }
    }
}

impl<'a> From<&'a Period<'a>> for api::Period<'a> {
    fn from(period: &'a Period<'a>) -> Self {
        Self {
            r#type: api::ValueType::from(&period.type_),
            value: period.value,
        }
    }
}

impl<'a> TryFrom<&'a Function<'a>> for api::Function<'a> {
    type Error = Utf8Error;

    fn try_from(function: &'a Function<'a>) -> Result<Self, Self::Error> {
        unsafe {
            let name = function.name.try_to_utf8()?;
            let system_name = function.system_name.try_to_utf8()?;
            let filename = function.filename.try_to_utf8()?;
            Ok(Self {
                name,
                system_name,
                filename,
                start_line: function.start_line,
            })
        }
    }
}

impl<'a> TryFrom<&'a Line<'a>> for api::Line<'a> {
    type Error = Utf8Error;

    fn try_from(line: &'a Line<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            function: api::Function::try_from(&line.function)?,
            line: line.line,
        })
    }
}

impl<'a> TryFrom<&'a Location<'a>> for api::Location<'a> {
    type Error = Utf8Error;

    fn try_from(location: &'a Location<'a>) -> Result<Self, Self::Error> {
        let mapping = api::Mapping::try_from(&location.mapping)?;
        let function = api::Function::try_from(&location.function)?;
        Ok(Self {
            mapping,
            function,
            address: location.address,
            line: location.line,
        })
    }
}

impl<'a> TryFrom<&'a Label<'a>> for api::Label<'a> {
    type Error = Utf8Error;

    fn try_from(label: &'a Label<'a>) -> Result<Self, Self::Error> {
        unsafe {
            let key = label.key.try_to_utf8()?;
            let str = label.str.try_to_utf8()?;
            let str = if str.is_empty() { None } else { Some(str) };
            let num_unit = label.num_unit.try_to_utf8()?;
            let num_unit = if num_unit.is_empty() {
                None
            } else {
                Some(num_unit)
            };

            Ok(Self {
                key,
                str,
                num: label.num,
                num_unit,
            })
        }
    }
}

impl<'a> TryFrom<Sample<'a>> for api::Sample<'a> {
    type Error = Utf8Error;

    fn try_from(sample: Sample<'a>) -> Result<Self, Self::Error> {
        let mut locations: Vec<api::Location> = Vec::with_capacity(sample.locations.len());
        unsafe {
            for location in sample.locations.as_slice().iter() {
                locations.push(location.try_into()?)
            }

            let values: Vec<i64> = sample.values.into_slice().to_vec();

            let mut labels: Vec<api::Label> = Vec::with_capacity(sample.labels.len());
            for label in sample.labels.as_slice().iter() {
                labels.push(label.try_into()?);
            }

            Ok(Self {
                locations,
                values,
                labels,
            })
        }
    }
}

/// Create a new profile with the given sample types. Must call
/// `ddog_prof_Profile_drop` when you are done with the profile.
///
/// # Arguments
/// * `sample_types`
/// * `period` - Optional period of the profile. Passing None/null translates to zero values.
/// * `start_time` - Optional time the profile started at. Passing None/null will use the current
///                  time.
///
/// # Safety
/// All slices must be have pointers that are suitably aligned for their type
/// and must have the correct number of elements for the slice.
#[no_mangle]
#[must_use]
pub unsafe extern "C" fn ddog_prof_Profile_new(
    sample_types: Slice<ValueType>,
    period: Option<&Period>,
    start_time: Option<&Timespec>,
    limits: ProfileLimits,
) -> ProfileNewResult {
    let types: Vec<api::ValueType> = sample_types.into_slice().iter().map(Into::into).collect();

    let limits = match api::Limits::try_from(limits) {
        Ok(ok) => ok,
        Err(_err) => {
            return ProfileNewResult::Err(Error::from(
                "ddog_prof_Profile_new failed due to bad limit values",
            ))
        }
    };

    let builder = profile::Profile::builder()
        .period(period.map(Into::into))
        .sample_types(types)
        .start_time(start_time.map(SystemTime::from))
        .limits(limits);

    match builder.build() {
        Ok(profile) => ProfileNewResult::Ok(Profile::from(profile)),
        Err(err) => ProfileNewResult::Err(Error::from(err.context("ddog_prof_Profile_new failed"))),
    }
}

#[cfg(test)]
impl From<ProfileNewResult> for Result<Profile, String> {
    fn from(result: ProfileNewResult) -> Self {
        match result {
            ProfileNewResult::Ok(ok) => Ok(ok),
            ProfileNewResult::Err(err) => Err(err.into()),
        }
    }
}

/// # Safety
/// The `profile` can be null, but if non-null it must point to a valid
/// Profile object created by this module.
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_Profile_drop(profile: *mut Profile) {
    if !profile.is_null() {
        std::ptr::drop_in_place(profile);
    }
}

#[cfg(test)]
impl From<ProfileResult> for Result<(), String> {
    fn from(result: ProfileResult) -> Self {
        match result {
            ProfileResult::Ok(_) => Ok(()),
            ProfileResult::Err(err) => Err(err.into()),
        }
    }
}

/// # Safety
/// The `profile` ptr must point to a valid Profile object created by this
/// module. All pointers inside the `sample` need to be valid for the duration
/// of this call.
///
/// If successful, it returns the Ok variant.
/// On error, it holds an error message in the error variant.
///
/// # Safety
/// The `profile` ptr must point to a valid Profile object created by this
/// module.
/// This call is _NOT_ thread-safe.
#[must_use]
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_Profile_add(
    profile: *mut Profile,
    sample: Sample,
) -> ProfileResult {
    ddog_prof_profile_add_impl(profile, sample).into_profile_result("ddog_prof_Profile_add failed")
}

unsafe fn ddog_prof_profile_add_impl(profile: *mut Profile, sample: Sample) -> anyhow::Result<()> {
    let profile = match profile.as_mut() {
        Some(p) => p.borrow_inner_mut(),
        None => anyhow::bail!("profile pointer was null"),
    };
    match sample.try_into().map(|s| profile.add(s)) {
        Ok(r) => match r {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        },
        Err(err) => Err(anyhow::Error::from(err)),
    }
}

/// Associate an endpoint to a given local root span id.
/// During the serialization of the profile, an endpoint label will be added
/// to all samples that contain a matching local root span id label.
///
/// Note: calling this API causes the "trace endpoint" and "local root span id" strings
/// to be interned, even if no matching sample is found.
///
/// # Arguments
/// * `profile` - a reference to the profile that will contain the samples.
/// * `local_root_span_id`
/// * `endpoint` - the value of the endpoint label to add for matching samples.
///
/// # Safety
/// The `profile` ptr must point to a valid Profile object created by this
/// module.
/// This call is _NOT_ thread-safe.
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_Profile_set_endpoint(
    profile: &mut Profile,
    local_root_span_id: u64,
    endpoint: CharSlice,
) -> ProfileResult {
    let profile = profile.borrow_inner_mut();
    let endpoint = endpoint.to_utf8_lossy();
    profile
        .add_endpoint(local_root_span_id, endpoint)
        .into_profile_result("ddog_prof_Profile_set_endpoint failed")
}

/// Count the number of times an endpoint has been seen.
///
/// # Arguments
/// * `profile` - a reference to the profile that will contain the samples.
/// * `endpoint` - the endpoint label for which the count will be incremented
///
/// # Safety
/// The `profile` ptr must point to a valid Profile object created by this
/// module.
/// This call is _NOT_ thread-safe.
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_Profile_add_endpoint_count(
    profile: &mut Profile,
    endpoint: CharSlice,
    value: i64,
) {
    let profile = profile.borrow_inner_mut();
    let endpoint = endpoint.to_utf8_lossy();
    profile.add_endpoint_count(endpoint, value);
}

/// Add a poisson-based upscaling rule which will be use to adjust values and make them
/// closer to reality.
///
/// # Arguments
/// * `profile` - a reference to the profile that will contain the samples.
/// * `offset_values` - offset of the values
/// * `label_name` - name of the label used to identify sample(s)
/// * `label_value` - value of the label used to identify sample(s)
/// * `sum_value_offset` - offset of the value used as a sum (compute the average with `count_value_offset`)
/// * `count_value_offset` - offset of the value used as a count (compute the average with `sum_value_offset`)
/// * `sampling_distance` - this is the threshold for this sampling window. This value must not be equal to 0
///
/// # Safety
/// This function must be called before serialize and must not be called after.
/// The `profile` ptr must point to a valid Profile object created by this
/// module.
/// This call is _NOT_ thread-safe.
#[must_use]
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_Profile_add_upscaling_rule_poisson(
    profile: &mut profile::Profile,
    offset_values: Slice<usize>,
    label_name: CharSlice,
    label_value: CharSlice,
    sum_value_offset: usize,
    count_value_offset: usize,
    sampling_distance: u64,
) -> ProfileResult {
    if sampling_distance == 0 {
        return ProfileResult::Err(ddcommon_ffi::Error::from(
            "sampling_distance parameter must be greater than 0",
        ));
    }
    let upscaling_info = api::UpscalingInfo::Poisson {
        sum_value_offset,
        count_value_offset,
        sampling_distance,
    };

    add_upscaling_rule(
        profile,
        offset_values,
        label_name,
        label_value,
        upscaling_info,
    )
    .into_profile_result("ddog_prof_Profile_add_upscaling_rule_poisson failed")
}

/// Add a proportional-based upscaling rule which will be use to adjust values and make them
/// closer to reality.
///
/// # Arguments
/// * `profile` - a reference to the profile that will contain the samples.
/// * `offset_values` - offset of the values
/// * `label_name` - name of the label used to identify sample(s)
/// * `label_value` - value of the label used to identify sample(s)
/// * `total_sampled` - number of sampled event (found in the pprof). This value must not be equal to 0
/// * `total_real` - number of events the profiler actually witnessed. This value must not be equal to 0
///
/// # Safety
/// This function must be called before serialize and must not be called after.
/// The `profile` ptr must point to a valid Profile object created by this
/// module.
/// This call is _NOT_ thread-safe.
#[must_use]
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_Profile_add_upscaling_rule_proportional(
    profile: &mut profile::Profile,
    offset_values: Slice<usize>,
    label_name: CharSlice,
    label_value: CharSlice,
    total_sampled: u64,
    total_real: u64,
) -> ProfileResult {
    if total_sampled == 0 || total_real == 0 {
        return ProfileResult::Err(ddcommon_ffi::Error::from(
            "total_sampled and total_real parameters must not be equal to 0",
        ));
    }

    let upscaling_info = api::UpscalingInfo::Proportional {
        scale: total_real as f64 / total_sampled as f64,
    };
    add_upscaling_rule(
        profile,
        offset_values,
        label_name,
        label_value,
        upscaling_info,
    )
    .into_profile_result("ddog_prof_Profile_add_upscaling_rule_proportional failed")
}

unsafe fn add_upscaling_rule(
    profile: &mut profile::Profile,
    offset_values: Slice<usize>,
    label_name: CharSlice,
    label_value: CharSlice,
    upscaling_info: api::UpscalingInfo,
) -> anyhow::Result<()> {
    let label_name_n = label_name.to_utf8_lossy();
    let label_value_n = label_value.to_utf8_lossy();
    profile.add_upscaling_rule(
        offset_values.as_slice(),
        label_name_n.as_ref(),
        label_value_n.as_ref(),
        upscaling_info,
    )
}

#[repr(C)]
pub struct EncodedProfile {
    start: Timespec,
    end: Timespec,
    buffer: ddcommon_ffi::Vec<u8>,
    endpoints_stats: Box<profiled_endpoints::ProfiledEndpointsStats>,
}

/// # Safety
/// Only pass a reference to a valid `ddog_prof_EncodedProfile` A valid
/// reference also means that it hasn't already been dropped. Do not call this
/// twice on the same object!
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_EncodedProfile_drop(profile: *mut EncodedProfile) {
    if !profile.is_null() {
        // Safety: EncodedProfile's are repr(C), and not box allocated. If the
        // user has followed the safety requirements of this function, then
        // this is safe.
        std::ptr::drop_in_place(profile);
    }
}

impl From<profile::EncodedProfile> for EncodedProfile {
    fn from(value: profile::EncodedProfile) -> Self {
        let start = value.start.into();
        let end = value.end.into();
        let buffer = value.buffer.into();
        let endpoints_stats = Box::new(value.endpoints_stats);

        Self {
            start,
            end,
            buffer,
            endpoints_stats,
        }
    }
}

/// Serialize the aggregated profile.
///
/// Don't forget to clean up the ok with `ddog_prof_EncodedProfile_drop` or
/// the error variant with `ddog_Error_drop` when you are done with them.
///
/// # Arguments
/// * `profile` - a reference to the profile being serialized.
/// * `end_time` - optional end time of the profile. If None/null is passed, the current time will
///                be used.
/// * `duration_nanos` - Optional duration of the profile. Passing None or a negative duration will
///                      mean the duration will based on the end time minus the start time, but
///                      under anomalous conditions this may fail as system clocks can be adjusted,
///                      or the programmer accidentally passed an earlier time. The duration of
///                      the serialized profile will be set to zero for these cases.
///
/// # Safety
/// The `profile` must point to a valid profile object.
/// The `end_time` must be null or otherwise point to a valid TimeSpec object.
/// The `duration_nanos` must be null or otherwise point to a valid i64.
#[must_use]
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_Profile_serialize(
    profile: &profile::Profile,
    end_time: Option<&Timespec>,
    duration_nanos: Option<&i64>,
) -> SerializeResult {
    let end_time = end_time.map(SystemTime::from);
    let duration = match duration_nanos {
        None => None,
        Some(x) if *x < 0 => None,
        Some(x) => Some(Duration::from_nanos((*x) as u64)),
    };
    match profile.serialize(end_time, duration) {
        Ok(ok) => SerializeResult::Ok(ok.into()),
        Err(err) => SerializeResult::Err(err.into()),
    }
}

#[must_use]
#[no_mangle]
pub unsafe extern "C" fn ddog_Vec_U8_as_slice(vec: &ddcommon_ffi::Vec<u8>) -> Slice<u8> {
    vec.as_slice()
}

/// Resets all data in `profile` except the sample types and period. Returns
/// true if it successfully reset the profile and false otherwise. The profile
/// remains valid if false is returned.
///
/// # Arguments
/// * `profile` - A mutable reference to the profile to be reset.
/// * `start_time` - The time of the profile (after reset). Pass None/null to use the current time.
///
/// # Safety
/// The `profile` must meet all the requirements of a mutable reference to the profile. Given this
/// can be called across an FFI boundary, the compiler cannot enforce this.
/// If `time` is not null, it must point to a valid Timespec object.
#[no_mangle]
pub unsafe extern "C" fn ddog_prof_Profile_reset(
    profile: &mut profile::Profile,
    start_time: Option<&Timespec>,
) -> bool {
    profile.reset(start_time.map(SystemTime::from)).is_ok()
}

#[cfg(test)]
mod test {
    use super::*;

    const MIB: u32 = 1024 * 1024;
    const TEST_LIMITS: ProfileLimits = ProfileLimits {
        functions_mem: MIB,
        locations_mem: MIB,
        mappings_mem: MIB,
        strings_mem: MIB,
    };

    #[test]
    fn ctor_and_dtor() {
        unsafe {
            let sample_type: *const ValueType = &ValueType::new("samples", "count");
            let result = ddog_prof_Profile_new(Slice::new(sample_type, 1), None, None, TEST_LIMITS);
            let mut profile = Result::from(result).unwrap();

            // This will drop the contents of the profile, leaving a null
            // pointer inside the profile wrapper, which is handled
            // accordingly when the wrapper is dropped.
            ddog_prof_Profile_drop(&mut profile);
        }
    }

    #[test]
    fn add_failure() {
        unsafe {
            let sample_type: *const ValueType = &ValueType::new("samples", "count");
            let result = ddog_prof_Profile_new(Slice::new(sample_type, 1), None, None, TEST_LIMITS);
            let mut profile = Result::from(result).unwrap();

            // wrong number of values (doesn't match sample types)
            let values: &[i64] = &[];

            let sample = Sample {
                locations: Slice::default(),
                values: Slice::from(values),
                labels: Slice::default(),
            };

            let result = Result::from(ddog_prof_Profile_add(&mut profile, sample));
            result.unwrap_err();
        }
    }

    #[test]
    fn aggregate_samples() {
        unsafe {
            let sample_type: *const ValueType = &ValueType::new("samples", "count");
            let result = ddog_prof_Profile_new(Slice::new(sample_type, 1), None, None, TEST_LIMITS);
            let mut profile = Result::from(result).unwrap();

            let mapping = Mapping {
                filename: "php".into(),
                ..Default::default()
            };

            let locations = vec![Location {
                mapping,
                function: Function {
                    name: "{main}".into(),
                    system_name: "{main}".into(),
                    filename: "index.php".into(),
                    start_line: 0,
                },
                ..Default::default()
            }];
            let values: Vec<i64> = vec![1];
            let labels = vec![Label {
                key: Slice::from("pid"),
                num: 101,
                ..Default::default()
            }];

            let sample = Sample {
                locations: Slice::from(&locations),
                values: Slice::from(&values),
                labels: Slice::from(&labels),
            };

            Result::from(ddog_prof_Profile_add(&mut profile, sample)).unwrap();
            assert_eq!(
                profile
                    .borrow_inner()
                    .only_for_testing_num_aggregated_samples(),
                1
            );

            Result::from(ddog_prof_Profile_add(&mut profile, sample)).unwrap();
            assert_eq!(
                profile
                    .borrow_inner()
                    .only_for_testing_num_aggregated_samples(),
                1
            );
        }
    }

    unsafe fn provide_distinct_locations_ffi() -> Profile {
        let sample_type: *const ValueType = &ValueType::new("samples", "count");
        let result = ddog_prof_Profile_new(Slice::new(sample_type, 1), None, None, TEST_LIMITS);
        let mut profile = Result::from(result).unwrap();

        let mapping = Mapping {
            filename: "php".into(),
            ..Default::default()
        };

        let main_locations = vec![Location {
            mapping,
            function: Function {
                name: "{main}".into(),
                system_name: "{main}".into(),
                filename: "index.php".into(),
                start_line: 0,
            },
            ..Default::default()
        }];
        let test_locations = vec![Location {
            mapping,
            function: Function {
                name: "test".into(),
                system_name: "test".into(),
                filename: "index.php".into(),
                start_line: 3,
            },
            line: 4,
            ..Default::default()
        }];
        let values: Vec<i64> = vec![1];
        let labels = vec![Label {
            key: Slice::from("pid"),
            str: Slice::from(""),
            num: 101,
            num_unit: Slice::from(""),
        }];

        let main_sample = Sample {
            locations: Slice::from(main_locations.as_slice()),
            values: Slice::from(values.as_slice()),
            labels: Slice::from(labels.as_slice()),
        };

        let test_sample = Sample {
            locations: Slice::from(test_locations.as_slice()),
            values: Slice::from(values.as_slice()),
            labels: Slice::from(labels.as_slice()),
        };

        Result::from(ddog_prof_Profile_add(&mut profile, main_sample)).unwrap();
        assert_eq!(
            profile
                .borrow_inner()
                .only_for_testing_num_aggregated_samples(),
            1
        );

        Result::from(ddog_prof_Profile_add(&mut profile, test_sample)).unwrap();
        assert_eq!(
            profile
                .borrow_inner()
                .only_for_testing_num_aggregated_samples(),
            2
        );

        profile
    }

    #[test]
    fn distinct_locations_ffi() {
        drop(unsafe { provide_distinct_locations_ffi() });
    }
}
