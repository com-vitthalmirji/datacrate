//! Resolved standard types. References transparently inherit the referent's shape.
use crate::{Contract, TypeShape};
use std::collections::{BTreeMap, HashMap};

macro_rules! primitives {
    ($($ty:ty => $name:literal),* $(,)?) => {$(
        impl Contract for $ty { const SHAPE: TypeShape = TypeShape::Primitive($name); }
    )*};
}
primitives! {
    bool => "bool", char => "char", str => "str", String => "String",
    i8 => "i8", i16 => "i16", i32 => "i32", i64 => "i64", i128 => "i128",
    u8 => "u8", u16 => "u16", u32 => "u32", u64 => "u64", u128 => "u128",
    isize => "isize", usize => "usize", f32 => "f32", f64 => "f64",
}
impl<T: Contract + ?Sized> Contract for &T {
    const SHAPE: TypeShape = T::SHAPE;
}
impl<T: Contract + ?Sized> Contract for &mut T {
    const SHAPE: TypeShape = T::SHAPE;
}
impl<T: Contract> Contract for Option<T> {
    const SHAPE: TypeShape = TypeShape::Optional(&T::SHAPE);
}
impl<T: Contract> Contract for Vec<T> {
    const SHAPE: TypeShape = TypeShape::Sequence(&T::SHAPE);
}
// Private whitelist keeps aliases valid without allowing arbitrary structured keys.
trait AtomicKey: Contract {}
macro_rules! keys {
    ($($ty:ty),* $(,)?) => {$(impl AtomicKey for $ty {})*};
}
keys!(
    bool, String, i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, isize, usize
);
impl<K: AtomicKey, V: Contract, S> Contract for HashMap<K, V, S> {
    const SHAPE: TypeShape = TypeShape::Map(&K::SHAPE, &V::SHAPE);
}
impl<K: AtomicKey, V: Contract> Contract for BTreeMap<K, V> {
    const SHAPE: TypeShape = TypeShape::Map(&K::SHAPE, &V::SHAPE);
}
