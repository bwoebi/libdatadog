// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present Datadog, Inc.

use super::{pad_to, AllocError};
use region::{Allocation, Protection};
use std::alloc::Layout;
use std::cell::Cell;
use std::ptr::NonNull;

pub struct ArenaAllocator {
    mapping: Allocation,
    remaining_capacity: Cell<usize>,

    // I would like this to be a const generic to avoid some runtime overhead
    // on every allocation. However,const generic expressions are not stable,
    // so you can't do very many things with them.
    alignment: usize,
}

impl ArenaAllocator {
    /// Creates an [ArenaAllocator] from the [Allocation]. Individual
    /// allocations will use the specified alignment, which needs to be a
    /// power of two.
    /// # Panics
    /// Panics if the combination of the alignment and allocation's size don't
    /// make sense, or if the alignment isn't a power of two.
    /// # Safety
    /// This uses the arena as if nobody else has already used it in some
    /// other way. As part of this, it needs all the bytes in the allocation
    /// to be zero filled.
    pub unsafe fn from_allocation(alignment: usize, allocation: Allocation) -> Self {
        let page_size = allocation.len();
        // Ensure the page size/alignment is not smaller than ALIGN.
        assert!(page_size >= alignment);
        assert!(alignment.is_power_of_two());

        Self {
            mapping: allocation,
            remaining_capacity: Cell::new(page_size),
            alignment,
        }
    }

    /// Creates an arena allocator whose underlying buffer holds at least
    /// `capacity` bytes. It will round up to a page size.
    pub fn with_capacity(alignment: usize, capacity: usize) -> anyhow::Result<Self> {
        // Check that the user isn't asking for rubbish, e.g.
        // Give me an arena that aligns to 1024 but only allocate 64 bytes.
        // It makes no sense, it's nonsense, fix it.
        assert!(alignment.is_power_of_two());
        assert_ne!(capacity, 0);
        assert!(capacity >= alignment);

        let region = region::alloc(capacity, Protection::READ_WRITE)?;

        // SAFETY: we haven't done any unsafe things with the region like give
        // out pointers to its interior bytse.
        Ok(unsafe { Self::from_allocation(alignment, region) })
    }

    pub fn remaining_capacity(&self) -> usize {
        self.remaining_capacity.get()
    }

    pub fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_zeroed(layout)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.align() > self.alignment {
            return Err(AllocError);
        }

        let size = layout.size();
        if size == 0 {
            return Ok(NonNull::slice_from_raw_parts(NonNull::dangling(), 0));
        }

        let padded_size = match pad_to(size, self.alignment) {
            Some(x) => x,
            None => return Err(AllocError),
        };

        let new_layout = unsafe { Layout::from_size_align_unchecked(padded_size, self.alignment) };

        let mut remaining_capacity = self.remaining_capacity.get();

        if new_layout.size() > remaining_capacity {
            return Err(AllocError);
        }
        let offset = self.mapping.len() - remaining_capacity;
        remaining_capacity -= new_layout.size();
        self.remaining_capacity.set(remaining_capacity);

        let base_ptr = self.mapping.as_ptr::<u8>() as *mut u8;

        // SAFETY: the allocation has already been determined to fit in the
        // region, so the addition will fit within the region, and will also
        // not be null.
        let allocated_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(offset)) };
        Ok(NonNull::slice_from_raw_parts(
            allocated_ptr,
            new_layout.size(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::page_size;

    #[test]
    #[should_panic]
    fn test_capacity_0() {
        _ = ArenaAllocator::with_capacity(1, 0);
    }

    #[test]
    #[should_panic]
    fn test_nonsense_capacity_alignment() {
        _ = ArenaAllocator::with_capacity(64, 24);
    }

    #[test]
    fn test_arena_basic_exhaustion() -> anyhow::Result<()> {
        let arena = ArenaAllocator::with_capacity(1, 1)?;

        let expected_size = page_size();
        let actual_size = arena.remaining_capacity();
        assert_eq!(expected_size, actual_size);

        // This should consume the whole arena.
        arena.allocate(Layout::from_size_align(expected_size, 1)?)?;

        // This should fail to allocate, zero bytes available.
        arena.allocate(Layout::new::<u8>()).unwrap_err();

        Ok(())
    }

    fn expect_distance(first: NonNull<[u8]>, second: NonNull<[u8]>, distance: usize) {
        let a = first.as_ptr() as *mut u8;
        let b = second.as_ptr() as *mut u8;

        assert_eq!(b, unsafe { a.add(distance) });
    }

    #[test]
    fn test_arena_basics() -> anyhow::Result<()> {
        const DISTANCE: usize = 8;
        let arena = ArenaAllocator::with_capacity(DISTANCE, DISTANCE * 4)?;

        // Four of these should fit.
        let layout = Layout::from_size_align(DISTANCE, DISTANCE)?;

        let first = arena.allocate(layout)?;
        let second = arena.allocate(layout)?;
        let third = arena.allocate(layout)?;
        let fourth = arena.allocate(layout)?;

        // This _may_ fail to allocate, because we're only guaranteed 32 bytes
        // but in practice, it won't fail because it's rounded to a page size,
        // and I've never seen pages that small, even for 16 bit. However, in
        // any case, it should not panic, which is the point of the call.
        _ = std::hint::black_box(arena.allocate(Layout::new::<u8>()));

        expect_distance(first, second, DISTANCE);
        expect_distance(second, third, DISTANCE);
        expect_distance(third, fourth, DISTANCE);

        Ok(())
    }

    #[test]
    fn test_arena_alignment() -> anyhow::Result<()> {
        const DISTANCE: usize = 16;
        let arena = ArenaAllocator::with_capacity(DISTANCE, DISTANCE * 2)?;

        // These should get padded to DISTANCE, as the arena alignment
        // supersedes the alignment of the item.
        let layout = Layout::from_size_align(DISTANCE / 2, DISTANCE / 2)?;

        let first = arena.allocate(layout)?;
        assert_eq!(DISTANCE, first.len());
        let second = arena.allocate(layout)?;
        assert_eq!(DISTANCE, second.len());

        expect_distance(first, second, DISTANCE);

        Ok(())
    }
}
