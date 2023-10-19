use region::Protection;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

struct Table<T> {
    /// Holds the allocated bytes.
    allocation: region::Allocation,

    /// The number of [T]s this table currently holds. Never decreases. Ever.
    len: AtomicU32,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for Table<T> {}

impl<T> Table<T> {
    fn new(capacity: u32) -> anyhow::Result<Self> {
        let n_bytes = capacity as usize * mem::size_of::<T>();
        let allocation = region::alloc(n_bytes, Protection::READ_WRITE)?;
        Ok(Self {
            allocation,
            len: AtomicU32::new(0),
            _marker: PhantomData,
        })
    }

    /// # Safety
    /// This function may only be called by a writer.
    #[inline(always)]
    unsafe fn alloc(&self, item: T) -> u32 {
        let offset = self.len.load(Ordering::Relaxed);
        if offset == (self.allocation.len() / mem::size_of::<T>()) as u32 {
            panic!("table is full");
        }

        let base = self.allocation.as_ptr::<T>() as *mut T;
        let addr = unsafe { base.add(offset as usize) };
        unsafe { std::ptr::write(addr, item) };

        // Must be done after item has been written.
        self.len.store(offset + 1, Ordering::Release);
        offset
    }

    /// # Safety
    /// This function may only be called by a writer.
    #[inline(always)]
    unsafe fn alloc_slice(&self, items: &[T]) -> &[T] {
        let offset = self.len.load(Ordering::Relaxed);
        let cap = (self.allocation.len() / mem::size_of::<T>()) as u32;
        // todo: fix panic
        let len = offset.checked_add(items.len() as u32).unwrap();
        if offset > cap - len {
            panic!("table is full");
        }

        let base = self.allocation.as_ptr::<T>() as *mut T;
        let addr = unsafe { base.add(offset as usize) };
        unsafe {
            libc::memcpy(
                addr as *mut libc::c_void,
                items.as_ptr() as *mut libc::c_void,
                items.len(),
            )
        };

        // Must be done after item has been written.
        self.len
            .store(offset + items.len() as u32, Ordering::Release);
        unsafe { std::slice::from_raw_parts(addr, items.len()) }
    }

    #[inline(never)]
    fn try_fetch(&self, offset: u32) -> anyhow::Result<&T> {
        let len = self.len.load(Ordering::Acquire);
        if offset < len {
            let base = self.allocation.as_ptr::<T>();
            let addr = unsafe { base.add(offset as usize) };
            Ok(unsafe { &*addr })
        } else {
            anyhow::bail!("table offset {offset} is out of bounds");
        }
    }

    #[inline(never)]
    fn try_fetch_range(&self, offset: u32, len: u32) -> anyhow::Result<&[T]> {
        let end = if let Some(x) = offset.checked_add(len) {
            x
        } else {
            anyhow::bail!("table offset {offset} + len {len} overflowed")
        };

        let table_len = self.len.load(Ordering::Acquire);
        if end < table_len {
            let base = self.allocation.as_ptr::<T>();
            let addr = unsafe { base.add(offset as usize) };
            Ok(unsafe { std::slice::from_raw_parts(addr, len as usize) })
        } else {
            anyhow::bail!("table offset {offset} is out of bounds");
        }
    }
}

pub struct TableWriter<T> {
    table: Arc<Table<T>>,
}

#[derive(Clone)]
pub struct TableReader<T> {
    table: Arc<Table<T>>,
}

pub fn new<T>(capacity: u32) -> anyhow::Result<(TableWriter<T>, TableReader<T>)> {
    let table = Arc::new(Table::new(capacity)?);

    let writer = TableWriter {
        table: Arc::clone(&table),
    };
    let reader = TableReader { table };

    Ok((writer, reader))
}

impl<T> TableWriter<T> {
    pub fn new(capacity: u32) -> anyhow::Result<Self> {
        let table = Arc::new(Table::new(capacity)?);

        let writer = TableWriter {
            table: Arc::clone(&table),
        };

        Ok(writer)
    }

    pub fn len(&self) -> usize {
        self.table.len.load(Ordering::Relaxed) as usize
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        let len = self.table.len.load(Ordering::Acquire);
        let base = self.table.allocation.as_ptr::<T>();
        unsafe { std::slice::from_raw_parts(base, len as usize) }.into_iter()
    }

    pub fn reader(&self) -> TableReader<T> {
        TableReader {
            table: Arc::clone(&self.table),
        }
    }

    pub fn add(&self, item: T) -> u32 {
        // SAFETY: Being called by a writer.
        unsafe { self.table.alloc(item) }
    }

    pub fn add_slice(&self, items: &[T]) -> &[T] {
        // SAFETY: Being called by a writer.
        unsafe { self.table.alloc_slice(items) }
    }

    #[inline(always)]
    pub fn try_fetch(&self, offset: u32) -> anyhow::Result<&T> {
        self.table.try_fetch(offset)
    }

    #[inline(always)]
    pub fn try_fetch_range(&self, offset: u32, len: u32) -> anyhow::Result<&[T]> {
        self.table.try_fetch_range(offset, len)
    }
}

impl<T> TableReader<T> {
    #[inline(always)]
    pub fn try_fetch(&self, offset: u32) -> anyhow::Result<&T> {
        self.table.try_fetch(offset)
    }

    #[inline(always)]
    pub fn try_fetch_range(&self, offset: u32, len: u32) -> anyhow::Result<&[T]> {
        self.table.try_fetch_range(offset, len)
    }
}
