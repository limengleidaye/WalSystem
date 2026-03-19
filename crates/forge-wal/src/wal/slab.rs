use anyhow::bail;
use anyhow::{Ok, Result};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlabState {
    None,
    Writing,
    Ready,
    InFlight,
    Completed,
}

/// 一个固定大小的内存块，多次写入共享同一个 buffer
pub struct Slab {
    buf: Vec<u8>,        // 固定大小的缓冲区
    pub capacity: usize, // buf 的总容量
    pub assigned: usize, // 已分配出去的偏移量（下一次写入的起始位置）
    written: usize,      // 已实际写完的字节数
    owner: usize,        // 这个 slab 当前属于哪个逻辑编号
    state: SlabState,
    notify: Arc<Notify>,
}

impl Slab {
    pub fn new(capacity: usize, owner: usize) -> Self {
        Self {
            buf: vec![0u8; capacity],
            capacity: capacity,
            assigned: 0,
            written: 0,
            owner,
            state: SlabState::None,
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn owner(&self) -> usize {
        self.owner
    }

    /// 尝试在 slab 中预留 capacity 大小的空间
    /// 返回 Some(offset) 表示预留成功，None 表示空间不足
    pub fn prepare_write(&mut self, capacity: usize) -> Option<usize> {
        if self.assigned + capacity > self.capacity {
            return None;
        }
        let offset = self.assigned;
        self.assigned += capacity;
        offset.into()
    }

    pub fn state(&self) -> SlabState {
        self.state
    }

    pub fn set_writing(&mut self) {
        if self.state == SlabState::None {
            self.state = SlabState::Writing;
        }
    }

    pub fn mark_ready(&mut self) {
        self.state = SlabState::Ready;
    }

    pub fn mark_inflight(&mut self) {
        self.state = SlabState::InFlight;
    }

    pub fn mark_completed(&mut self) {
        self.state = SlabState::Completed;
    }

    pub fn notify_handle(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    pub fn notify_waiters(&self) {
        self.notify.notify_waiters();
    }

    pub fn remaining(&self) -> usize {
        self.capacity - self.assigned
    }

    /// 获取 slab 内指定区域的可写切片
    pub fn slice_mut(&mut self, offset: usize, len: usize) -> &mut [u8] {
        &mut self.buf[offset..offset + len]
    }

    /// 标记一段写入完成
    pub fn complete_write(&mut self, len: usize) {
        self.written += len
    }

    /// slab 是否已经写满（assigned == capacity）
    pub fn is_full(&self) -> bool {
        self.assigned >= self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.assigned == 0
    }

    /// 所有已分配的写入是否都已完成
    pub fn is_ready(&self) -> bool {
        self.state == SlabState::Ready
    }

    pub fn len_for_flush(&self) -> usize {
        self.assigned
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    pub fn pad_zeroes(&mut self) {
        if self.assigned == self.capacity {
            return;
        }
        let remaining = self.capacity - self.assigned;
        self.buf[self.assigned..self.capacity].fill(0);
        self.assigned = self.capacity;
        self.complete_write(remaining);
    }

    /// 重置 slab ,赋予新的 owner 编号以供环的下一轮复用
    pub fn reset(&mut self, new_owner: usize) {
        self.assigned = 0;
        self.written = 0;
        self.owner = new_owner;
        self.state = SlabState::None;
    }

    pub fn ensure_owner(&self, expected: usize) -> Result<()> {
        if self.owner != expected {
            bail!(
                "slab owner mismatch, expected {}, actual {}",
                expected,
                self.owner
            );
        }
        Ok(())
    }
}
