pub mod io;
pub mod ring;
pub mod slab;
pub mod system;

use std::ops::{Deref, DerefMut};

#[repr(align(64))]
pub struct CL<T>(pub T);
impl<T> Deref for CL<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for CL<T> {
    fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
        &mut self.0
    }
}

impl<T> From<T> for CL<T> {
    fn from(value: T) -> CL<T> {
        CL(value)
    }
}
