pub trait IntoLossy<T>: Sized {
    #[must_use]
    fn into_lossy(self) -> T;
}

pub trait FromLossy<T>: Sized {
    #[must_use]
    fn from_lossy(value: T) -> Self;
}

impl<T, U> IntoLossy<U> for T
where
    U: FromLossy<T>,
{
    #[track_caller]
    #[inline]
    fn into_lossy(self) -> U {
        U::from_lossy(self)
    }
}

impl<T> FromLossy<T> for T {
    #[inline(always)]
    fn from_lossy(value: T) -> T {
        value
    }
}

macro_rules! impl_from_lossy {
    ($src:ty => $target:ty) => {
        impl FromLossy<$src> for $target {
            #[inline(always)]
            fn from_lossy(value: $src) -> Self {
                value as Self
            }
        }
    };
}

impl_from_lossy!(u8 => u16);
impl_from_lossy!(u8 => u32);
impl_from_lossy!(u8 => u64);
impl_from_lossy!(u8 => u128);
impl_from_lossy!(u8 => usize);
impl_from_lossy!(u16 => u8);
impl_from_lossy!(u16 => u32);
impl_from_lossy!(u16 => u64);
impl_from_lossy!(u16 => u128);
impl_from_lossy!(u16 => usize);
impl_from_lossy!(u32 => u8);
impl_from_lossy!(u32 => u16);
impl_from_lossy!(u32 => u64);
impl_from_lossy!(u32 => u128);
impl_from_lossy!(u32 => usize);
impl_from_lossy!(u64 => u8);
impl_from_lossy!(u64 => u16);
impl_from_lossy!(u64 => u32);
impl_from_lossy!(u64 => u128);
impl_from_lossy!(u64 => usize);
impl_from_lossy!(u128 => u8);
impl_from_lossy!(u128 => u16);
impl_from_lossy!(u128 => u32);
impl_from_lossy!(u128 => u64);
impl_from_lossy!(u128 => usize);

impl_from_lossy!(i8 => i16);
impl_from_lossy!(i8 => i32);
impl_from_lossy!(i8 => i64);
impl_from_lossy!(i8 => i128);
impl_from_lossy!(i8 => isize);
impl_from_lossy!(i16 => i8);
impl_from_lossy!(i16 => i32);
impl_from_lossy!(i16 => i64);
impl_from_lossy!(i16 => i128);
impl_from_lossy!(i16 => isize);
impl_from_lossy!(i32 => i8);
impl_from_lossy!(i32 => i16);
impl_from_lossy!(i32 => i64);
impl_from_lossy!(i32 => i128);
impl_from_lossy!(i32 => isize);
impl_from_lossy!(i64 => i8);
impl_from_lossy!(i64 => i16);
impl_from_lossy!(i64 => i32);
impl_from_lossy!(i64 => i128);
impl_from_lossy!(i64 => isize);
impl_from_lossy!(i128 => i8);
impl_from_lossy!(i128 => i16);
impl_from_lossy!(i128 => i32);
impl_from_lossy!(i128 => i64);
impl_from_lossy!(i128 => isize);

impl_from_lossy!(usize => u8);
impl_from_lossy!(usize => u16);
impl_from_lossy!(usize => u32);
impl_from_lossy!(usize => u64);
impl_from_lossy!(usize => u128);

impl_from_lossy!(isize => i8);
impl_from_lossy!(isize => i16);
impl_from_lossy!(isize => i32);
impl_from_lossy!(isize => i64);
impl_from_lossy!(isize => i128);
