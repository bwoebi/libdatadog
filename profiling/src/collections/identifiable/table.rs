// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present Datadog, Inc.

use super::*;
use crate::alloc::{AllocError, ArenaAllocator};
use ouroboros::self_referencing;
use region::Protection;
use std::alloc::Layout;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::mem::MaybeUninit;

struct BorrowedTable<'a, T: Item> {
    allocator: &'a ArenaAllocator,
    items: *mut MaybeUninit<T>,
    map: HashMap<&'a T, T::Id, BuildHasherDefault<rustc_hash::FxHasher>>,
}

#[self_referencing]
struct TableCell<T: Item + 'static> {
    owner: ArenaAllocator,

    #[borrows(owner)]
    #[covariant]
    dependent: BorrowedTable<'this, T>,
}

pub struct Table<T: Item + 'static> {
    // ouroboros will add a lot of functions to this struct, which we don't
    // want to expose publicly, so the internals are wrapped and private.
    inner: TableCell<T>,
}

impl<T: Item + 'static> Table<T> {
    ///
    #[inline]
    pub fn with_arena_capacity(capacity: usize) -> Result<Self, AllocError> {
        let layout = Layout::new::<T>();

        let allocator_capacity = match layout.size().checked_mul(capacity) {
            Some(c) => c,
            None => return Err(AllocError),
        };

        let mut allocation = match region::alloc(allocator_capacity, Protection::READ_WRITE) {
            Ok(a) => a,
            Err(_err) => return Err(AllocError),
        };

        let base_ptr = allocation.as_mut_ptr::<MaybeUninit<T>>();

        assert_eq!(base_ptr.align_offset(layout.align()), 0);

        // SAFETY: safe, probably. We're doing some unsafe things, but I think
        // they are compatible.
        let allocator = unsafe { ArenaAllocator::from_allocation(layout.align(), allocation) };

        let inner = TableCell::new(allocator, |allocator| BorrowedTable {
            allocator,
            items: base_ptr,
            map: Default::default(),
        });

        Ok(Self { inner })
    }

    #[allow(unused)]
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.with_dependent(|table| table.map.len())
    }

    #[allow(unused)]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get_id(&self, id: T::Id) -> T {
        self.inner.with_dependent(|table| {
            let offset = id.to_offset();
            let len = table.map.len();
            assert!(offset <= len);

            // SAFETY: we've checked that it fits into the allocated range, so
            // it should be in range _and_ initialized.
            unsafe {
                let slot = table.items.add(offset);
                slot.read().assume_init()
            }
        })
    }

    #[inline]
    pub fn insert(&mut self, item: T) -> Result<T::Id, AllocError> {
        self.insert_full(item).map(|t| t.0)
    }

    #[inline]
    pub fn insert_full(&mut self, item: T) -> Result<(T::Id, bool), AllocError> {
        self.inner
            .with_dependent_mut(|table| match table.map.get(&item) {
                None => {
                    let id = T::Id::from_offset(table.map.len());
                    let address = table.allocator.allocate(Layout::for_value(&item))?;

                    // Write the item into the newly allocated memory.
                    let dst = address.as_ptr() as *mut T;
                    debug_assert_eq!(dst.align_offset(std::mem::align_of::<T>()), 0);
                    unsafe { std::ptr::copy(&item, dst, 1) }

                    // SAFETY: item was initialized into allocator mem above.
                    table.map.insert(unsafe { &*dst }, id);
                    Ok((id, true))
                }
                Some(id) => Ok((*id, false)),
            })
    }

    /// Returns an iterator over the strings in the table. The items are
    /// returned in the order they were inserted, matching the [Id]s.
    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.inner.with_dependent(|table| {
            let len = table.map.len();

            // SAFETY: we've constrained the range to be limited to the
            // initialized values.
            unsafe {
                let items = std::slice::from_raw_parts_mut(table.items, len);
                items.iter().map(|init| init.assume_init_read())
            }
        })
    }

    pub fn to_pprof_vec(&self) -> Vec<T::PprofMessage>
    where
        T: PprofItem,
    {
        self.iter()
            .enumerate()
            .map(|(index, item)| item.to_pprof(<T as Item>::Id::from_offset(index)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
    struct TestId(usize);

    impl Id for TestId {
        type RawId = u64;

        fn from_offset(inner: usize) -> Self {
            Self(inner)
        }

        fn to_offset(&self) -> usize {
            self.0
        }

        fn to_raw_id(&self) -> Self::RawId {
            Self::RawId::try_from(self.0).expect("test id to fit into u64")
        }
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
    struct TestItem {
        name: StringId,
    }

    impl Item for TestItem {
        type Id = TestId;
    }

    impl PprofItem for TestItem {
        type PprofMessage = TestPprofMessage;

        fn to_pprof(&self, id: Self::Id) -> Self::PprofMessage {
            Self::PprofMessage {
                id: id.to_raw_id(),
                name: self.name.to_raw_id(),
            }
        }
    }

    #[derive(Message, Eq, PartialEq)]
    struct TestPprofMessage {
        #[prost(uint64, tag = "1")]
        pub id: u64,

        #[prost(int64, tag = "2")]
        pub name: i64, // Index into string table
    }

    #[test]
    fn test_operations() -> anyhow::Result<()> {
        let items = [
            TestItem {
                name: StringId::from_offset(3),
            },
            TestItem {
                name: StringId::from_offset(13),
            },
        ];

        let mut table = Table::with_arena_capacity(2)?;

        for item in items {
            table.insert(item)?;
        }

        assert_eq!(table.get_id(TestId::from_offset(0)), items[0]);
        assert_eq!(table.get_id(TestId::from_offset(1)), items[1]);

        let actual_items = table.to_pprof_vec();
        let expected_items = vec![
            TestPprofMessage { id: 0, name: 3 },
            TestPprofMessage { id: 1, name: 13 },
        ];

        assert_eq!(expected_items, actual_items);

        Ok(())
    }
}
