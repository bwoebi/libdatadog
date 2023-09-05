// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present Datadog, Inc.

use super::*;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum LabelValue {
    Str(StringId),
    Num {
        num: i64,
        num_unit: Option<StringId>,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Label {
    key: StringId,
    value: LabelValue,
}

impl Label {
    pub fn has_num_value(&self) -> bool {
        matches!(self.value, LabelValue::Num { .. })
    }

    pub fn has_string_value(&self) -> bool {
        matches!(self.value, LabelValue::Str(_))
    }

    pub fn get_key(&self) -> StringId {
        self.key
    }

    pub fn get_value(&self) -> &LabelValue {
        &self.value
    }

    pub fn num(key: StringId, num: i64, num_unit: Option<StringId>) -> Self {
        Self {
            key,
            value: LabelValue::Num { num, num_unit },
        }
    }

    pub fn str(key: StringId, v: StringId) -> Self {
        Self {
            key,
            value: LabelValue::Str(v),
        }
    }
}

impl From<Label> for crate::profile::serializer::Label {
    fn from(l: Label) -> Self {
        let key = l.key.to_raw_id() as u32;
        match l.value {
            LabelValue::Str(str) => Self {
                key,
                str: str.to_raw_id() as u32,
                num: 0,
                num_unit: 0,
            },
            LabelValue::Num { num, num_unit } => Self {
                key,
                str: 0,
                num,
                num_unit: num_unit.map_or(0, |u| u.to_raw_id() as u32),
            },
        }
    }
}

impl From<Label> for pprof::Label {
    fn from(l: Label) -> Self {
        Self::from(&l)
    }
}

impl From<&Label> for pprof::Label {
    fn from(l: &Label) -> pprof::Label {
        let key = l.key.to_raw_id();
        match l.value {
            LabelValue::Str(str) => Self {
                key,
                str: str.to_raw_id(),
                num: 0,
                num_unit: 0,
            },
            LabelValue::Num { num, num_unit } => Self {
                key,
                str: 0,
                num,
                num_unit: num_unit.map(StringId::into_raw_id).unwrap_or_default(),
            },
        }
    }
}

impl Item for Label {
    type Id = LabelId;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct LabelId(u32);

impl Id for LabelId {
    type RawId = usize;

    fn from_offset(inner: usize) -> Self {
        let index: u32 = inner.try_into().expect("LabelId to fit into a u32");
        Self(index)
    }

    fn to_raw_id(&self) -> Self::RawId {
        self.0 as Self::RawId
    }
}
impl LabelId {
    #[inline]
    pub fn to_offset(&self) -> usize {
        self.0 as usize
    }
}

/// A canonical representation for sets of labels.
/// You should only use the impl functions to modify this.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct LabelSet {
    // Guaranteed to be sorted by [Self::new]
    sorted_labels: Box<[LabelId]>,
}

impl From<LabelSet> for crate::profile::serializer::LabelSet {
    fn from(ls: LabelSet) -> Self {
        Self {
            labels: ls
                .sorted_labels
                .iter()
                .map(|l| l.to_raw_id() as u32)
                .collect(),
        }
    }
}

impl LabelSet {
    pub fn iter(&self) -> core::slice::Iter<'_, LabelId> {
        self.sorted_labels.iter()
    }

    pub fn new(mut v: Vec<LabelId>) -> Self {
        v.sort_unstable();
        let sorted_labels = v.into_boxed_slice();
        Self { sorted_labels }
    }
}

impl Item for LabelSet {
    type Id = LabelSetId;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct LabelSetId(u32);

impl Id for LabelSetId {
    type RawId = usize;

    fn from_offset(inner: usize) -> Self {
        let index: u32 = inner.try_into().expect("LabelSetId to fit into a u32");
        Self(index)
    }

    fn to_raw_id(&self) -> Self::RawId {
        self.0 as Self::RawId
    }
}

impl LabelSetId {
    #[inline]
    pub fn to_offset(&self) -> usize {
        self.0 as usize
    }
}
