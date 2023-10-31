// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present Datadog, Inc.

use super::*;

use crate::alloc::{AllocError, TableReader, TableWriter};
use ouroboros::self_referencing;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::mem::transmute;

#[cfg(test)]
use std::ops::Range;

struct BorrowedStringTable<'a> {
    /// The arena to store the characters in.
    bytes: &'a TableWriter<u8>,

    /// Used to have efficient lookup by [StringId], and to provide an
    /// [Iterator] over the strings.
    vec: TableWriter<&'a str>,

    /// Used to have efficient lookup by [&str].
    map: HashMap<&'a str, StringId, BuildHasherDefault<rustc_hash::FxHasher>>,
}

#[self_referencing]
struct StringTableCell {
    owner: TableWriter<u8>,

    #[borrows(owner)]
    #[covariant]
    dependent: BorrowedStringTable<'this>,
}

/// The [StringTable] stores strings and associates them with [StringId]s,
/// which correspond to the order in which strings were inserted. The empty
/// string is always associated with [StringId::ZERO].
pub struct StringTable {
    // ouroboros will add a lot of functions to this struct, which we don't
    // want to expose publicly, so the internals are wrapped and private.
    inner: StringTableCell,
}

#[self_referencing]
struct StringTableReaderCell {
    owner: TableReader<u8>,
    #[borrows(owner)]
    #[covariant]
    dependent: TableReader<&'this str>,
}

unsafe impl Send for StringTableReaderCell {}

pub struct StringTableReader {
    cell: StringTableReaderCell,
}

impl StringTableReader {
    unsafe fn new(reader: TableReader<u8>, string_reader: TableReader<&str>) -> Self {
        let cell =
            StringTableReaderCell::new(reader, |_bytes_reader| unsafe { transmute(string_reader) });

        StringTableReader { cell }
    }

    pub fn try_get_id(&self, id: StringId) -> anyhow::Result<&str> {
        let string_reader = self.cell.borrow_dependent();
        string_reader.try_fetch(id.to_offset() as u32).copied()
    }
}

impl StringTable {
    /// Creates a new [StringTable] with the given max capacity, which may be
    /// rounded up to a convenient number for the underlying allocator.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Result<Self, AllocError> {
        let bytes = match TableWriter::new(capacity as u32) {
            Ok(ok) => ok,
            Err(_err) => return Err(AllocError),
        };

        let inner = StringTableCell::new(bytes, |bytes| BorrowedStringTable {
            bytes,
            // todo: fix hard-coded number and panic
            vec: TableWriter::new(4096).unwrap(),
            map: Default::default(),
        });

        let mut s = Self { inner };
        // string tables always have the empty string at 0.
        let (_id, _inserted) = s.insert_full("")?;
        debug_assert!(_id == StringId::ZERO);
        debug_assert!(_inserted);
        Ok(s)
    }

    #[allow(unused)]
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.with_dependent(|table| table.vec.len())
    }

    pub fn at_watermark(&self, level: f64) -> bool {
        self.inner.with_dependent(|table| {
            table.bytes.at_watermark(level) || table.vec.at_watermark(level)
        })
    }

    #[allow(unused)]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Inserts the string into the table, if it did not already exist. The id
    /// of the string is returned.
    ///
    /// # Panics
    /// Panics if a new string needs to be inserted but the offset of the new
    /// string doesn't fit into a [StringId].
    #[inline]
    pub fn insert(&mut self, str: &str) -> Result<StringId, AllocError> {
        self.insert_full(str).map(|t| t.0)
    }

    /// Inserts the string into the table, if it did not already exist. The id
    /// of the string is returned, along with whether the string was inserted.
    ///
    /// # Panics
    /// Panics if a new string needs to be inserted but the offset of the new
    /// string doesn't fit into a [StringId].
    #[inline]
    pub fn insert_full(&mut self, str: &str) -> Result<(StringId, bool), AllocError> {
        // For performance, delay converting the &str to a String until after
        // it has been determined to not exist in the set. This avoids
        // temporary allocations.
        self.inner
            .with_dependent_mut(|table| match table.map.get(str) {
                None => {
                    let id = StringId::from_offset(table.vec.len());
                    let slice = table.bytes.add_slice(str.as_bytes());

                    // SAFETY: the buffer was copied from a valid string, so
                    // the copy must also be valid.
                    let allocated_str = unsafe { std::str::from_utf8_unchecked(slice) };

                    table.vec.add(allocated_str);
                    table.map.insert(allocated_str, id);
                    assert_eq!(table.vec.len(), table.map.len());
                    Ok((id, true))
                }
                Some(id) => Ok((*id, false)),
            })
    }

    /// Gets the string associated with the id.
    ///
    /// # Panics
    /// Panics if the [StringId] doesn't exist in the table.
    #[inline]
    pub fn get_id(&self, id: StringId) -> &str {
        self.inner.with_dependent(|table| {
            let offset = id.to_offset();
            // todo: should this take by usize or..?
            match table.vec.try_fetch(offset as u32) {
                Ok(item) => *item,
                Err(_) => panic!("expected string id {offset} to exist in the string table"),
            }
        })
    }

    pub fn get_reader(&self) -> StringTableReader {
        self.inner.with_dependent(|table| unsafe {
            StringTableReader::new(table.bytes.reader(), table.vec.reader())
        })
    }

    #[cfg(test)]
    #[allow(unused)]
    #[inline]
    pub fn get_range(&self, range: Range<usize>) -> &[&str] {
        self.inner.with_dependent(|table| {
            table
                .vec
                .try_fetch_range(range.start as u32, (range.end - range.start) as u32)
                .unwrap()
        })
    }

    /// Returns an iterator over the strings in the table. The items are
    /// returned in the order they were inserted, matching the [StringId]s.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.inner.with_dependent(|table| table.vec.iter().copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_string_table() -> anyhow::Result<()> {
        let cases: &[_] = &[
            (StringId::ZERO, ""),
            (StringId::from_offset(1), "local root span id"),
            (StringId::from_offset(2), "span id"),
            (StringId::from_offset(3), "trace endpoint"),
            (StringId::from_offset(4), "samples"),
            (StringId::from_offset(5), "count"),
            (StringId::from_offset(6), "wall-time"),
            (StringId::from_offset(7), "nanoseconds"),
            (StringId::from_offset(8), "cpu-time"),
            (StringId::from_offset(9), "<?php"),
            (StringId::from_offset(10), "/srv/demo/public/index.php"),
            (StringId::from_offset(11), "pid"),
        ];

        let capacity = cases.iter().map(|(_, str)| str.len()).sum();

        let mut set = StringTable::with_capacity(capacity)?;

        // the empty string must always be included in the set at 0.
        let empty_str = set.get_id(StringId::ZERO);
        assert_eq!("", empty_str);

        for (offset, str) in cases.iter() {
            let actual_offset = set.insert(str)?;
            assert_eq!(*offset, actual_offset);
        }

        // repeat them to ensure they aren't re-added
        for (offset, str) in cases.iter() {
            let actual_offset = set.insert(str)?;
            assert_eq!(*offset, actual_offset);
        }

        // let's fetch by offset
        for (id, expected_string) in cases.iter().cloned() {
            assert_eq!(expected_string, set.get_id(id));
        }

        // Check a range too
        let slice = set.get_range(7..10);
        let expected_slice = &["nanoseconds", "cpu-time", "<?php"];
        assert_eq!(expected_slice, slice);

        // And the whole set:
        assert_eq!(cases.len(), set.len());
        let actual = set
            .iter()
            .enumerate()
            .map(|(offset, item)| (StringId::from_offset(offset), item))
            .collect::<Vec<_>>();
        assert_eq!(cases, &actual);
        Ok(())
    }

    #[test]
    fn test_reader() -> anyhow::Result<()> {
        let mut strings = StringTable::with_capacity(1024)?;
        let florian = strings.insert("Florian")?;
        let levi = strings.insert("Levi")?;

        let reader = strings.get_reader();
        let handle = std::thread::spawn(move || {
            let f = reader.try_get_id(florian).unwrap();
            let l = reader.try_get_id(levi).unwrap();

            assert_eq!("Florian", f);
            assert_eq!("Levi", l);
        });

        if let Err(err) = handle.join() {
            anyhow::bail!("Failed to join: {err:?}")
        }
        Ok(())
    }
}
