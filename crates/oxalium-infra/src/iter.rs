use std::iter::repeat_n;

#[inline]
pub fn distribute(amount: usize, part: usize) -> impl Iterator<Item = usize> {
    let v = amount / part;
    let k = amount % part;
    repeat_n(v + 1, k).chain(repeat_n(v, part - k))
}
