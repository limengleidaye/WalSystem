use anyhow::Result;
use anyhow::bail;
use std::intrinsics::write_bytes;
use std::slice::from_raw_parts_mut;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::Notify;

const SLAB_BITS: usize = 4;
const SLAB_MASK: usize = (1 << SLAB_BITS) - 1;

pub const SLAB_NONE: usize = 0b0000;
pub const SLAB_WRITING: usize = 0b0001;
pub const SLAB_READY: usize = 0b0010;
pub const SLAB_IN_FLIGHT: usize = 0b0100;
pub const SLAB_COMPLETED: usize = 0b1000;

/// 一个固定大小的内存块，多次写入共享同一个 buffer
pub struct Slab {
    buf: Vec<u8>,                 // 固定大小的缓冲区
    state: AtomicU64,             // 打包 owner(高位) + state(低4位)
    capacity: usize,              // buf 的总容量
    assigned: AtomicUsize,        // 已分配出去的偏移量（下一次写入的起始位置）
    written: AtomicUsize,         // 已实际写完的字节数
    pending_writers: AtomicUsize, // 当前活跃的写入者数量
    waker: Notify,                // 唤醒等待持久化的 producer
}

impl Slab {
    pub fn new(capacity: usize, owner: usize) -> Self {
        Self {
            buf: vec![0u8; capacity],
            state: AtomicU64::new(Self::pack(owner, SLAB_NONE)),
            capacity: capacity,
            assigned: AtomicUsize::new(0),
            written: AtomicUsize::new(0),
            pending_writers: AtomicUsize::new(0),
            waker: Notify::new(),
        }
    }

    fn pack(owner: usize, state: usize) -> u64 {
        ((owner << SLAB_BITS) | (state & SLAB_MASK)) as u64
    }

    fn unpack(v: u64) -> (usize, usize) {
        let v = v as usize;
        (v >> SLAB_BITS, v & SLAB_MASK)
    }

    /// 读取当前 (owner, state)
    pub fn inspect(&self) -> (usize, usize) {
        Self::unpack(self.state.load(Ordering::Acquire))
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    // ---- 引用计数 ----

    pub fn acquire_ref(&self) {
        self.pending_writers.fetch_add(1, Ordering::Relaxed);
    }

    pub fn release_ref(&self) {
        self.pending_writers.fetch_sub(1, Ordering::Release);
    }

    /// 所有写入者都已退出
    pub fn is_reusable(&self) -> bool {
        self.pending_writers.load(Ordering::Acquire) == 0
    }

    // ---- 状态转换 ----

    /// CAS 尝试 NONE → WRITING，成功返回 Ok，失败返回当前 (owner, state)
    pub fn try_claim(&self, owner: usize) -> Result<(), (usize, usize)> {
        match self.state.compare_exchange(
            Self::pack(owner, SLAB_NONE),
            Self::pack(owner, SLAB_WRITING),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(state) => Err(Self::unpack(state)),
        }
    }

    pub fn mark_ready(&self, owner: usize) {
        self.state
            .store(Self::pack(owner, SLAB_READY), Ordering::Release);
    }

    pub fn mark_inflight(&self, owner: usize) {
        self.state
            .store(Self::pack(owner, SLAB_IN_FLIGHT), Ordering::Release);
    }

    pub fn mark_completed(&self, owner: usize) {
        self.state
            .store(Self::pack(owner, SLAB_COMPLETED), Ordering::Release);
    }

    // ---- 写入操作 ----

    /// 尝试在 slab 中预留 capacity 大小的空间
    /// 返回 Some(offset) 表示预留成功，None 表示空间不足
    pub fn prepare_write(&self, capacity: usize) -> usize {
        self.assigned.fetch_add(capacity, Ordering::Release)
    }

    /// 获取 slab 内指定区域的可写切片（调用方需保证不与其他写入者重叠）
    pub fn slice_at(&self, offset: usize, len: usize) -> &mut [u8] {
        unsafe {
            let ptr = self.buf.as_ptr().add(offset) as *mut u8;
            from_raw_parts_mut(ptr, len)
        }
    }

    /// 标记一段写入完成，返回写完后的 written 总量
    pub fn complete_write(&self, len: usize) -> usize {
        self.written.fetch_add(len, Ordering::Release) + len
    }

    pub fn submit_with_padding(&self, assigned: usize) -> usize {
        let padding = self.capacity - assigned;
        unsafe {
            let ptr = self.buf.as_ptr().add(assigned) as *mut u8;
            write_bytes(ptr, 0, padding);
        };
        self.complete_write(padding)
    }

    pub fn len_for_flush(&self) -> usize {
        // 当 slab READY 时，assigned 已经等于 capacity
        self.capacity
    }

    /// ---- 唤醒 ----
    pub fn waker(&self) -> &Notify {
        &self.waker
    }

    pub fn notify_waiters(&self) {
        self.waker.notify_waiters();
    }

    /// 重置 slab ,赋予新的 owner 编号以供环的下一轮复用
    pub fn reset(&self, new_owner: usize) {
        self.assigned.store(0, Ordering::Relaxed);
        self.written.store(0, Ordering::Relaxed);
        self.state
            .store(Self::pack(new_owner, SLAB_NONE), Ordering::Relaxed);
    }
}
