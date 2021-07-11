// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::array::StringArray;
use common_arrow::arrow::compute::kernels::comparison;
use common_arrow::arrow::compute::*;
use common_exception::ErrorCode;
use common_exception::Result;
use num::Num;
use num::NumCast;
use num::ToPrimitive;

use super::DataArray;
use crate::arrays::*;
use crate::series::Series;
use crate::utils::NoNull;
use crate::*;

pub trait ArrayCompare<Rhs>: Debug {
    /// Check for equality and regard missing values as equal.
    fn eq_missing(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: eq_missing for {:?}",
            self,
        )))
    }

    /// Check for equality.
    fn eq(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: eq for {:?}",
            self,
        )))
    }

    /// Check for inequality.
    fn neq(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: eq for {:?}",
            self,
        )))
    }

    /// Greater than comparison.
    fn gt(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: gt for {:?}",
            self,
        )))
    }

    /// Greater than or equal comparison.
    fn gt_eq(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: gt_eq for {:?}",
            self,
        )))
    }

    /// Less than comparison.
    fn lt(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: lt for {:?}",
            self,
        )))
    }

    /// Less than or equal comparison
    fn lt_eq(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: lt_eq for {:?}",
            self,
        )))
    }

    fn like(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: like for {:?}",
            self,
        )))
    }

    fn nlike(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: nlike for {:?}",
            self,
        )))
    }
}

impl<T> DataArray<T>
where T: DFNumericType
{
    /// First ensure that the Arrays of lhs and rhs match and then iterates over the Arrays and applies
    /// the comparison operator.
    fn comparison(
        &self,
        rhs: &DataArray<T>,
        operator: impl Fn(
            &PrimitiveArray<T>,
            &PrimitiveArray<T>,
        ) -> common_arrow::arrow::error::Result<BooleanArray>,
    ) -> Result<DFBooleanArray> {
        let array = Arc::new(operator(self.downcast_ref(), rhs.downcast_ref())?) as ArrayRef;
        Ok(array.into())
    }
}

macro_rules! impl_eq_missing {
    ($self:ident, $rhs:ident) => {{
        match ($self.null_count(), $rhs.null_count()) {
            (0, 0) => $self
                .into_no_null_iter()
                .zip($rhs.into_no_null_iter())
                .map(|(opt_a, opt_b)| opt_a == opt_b)
                .collect(),
            (_, _) => $self
                .downcast_iter()
                .zip($rhs.downcast_iter())
                .map(|(opt_a, opt_b)| opt_a == opt_b)
                .collect(),
        }
    }};
}

macro_rules! apply {
    ($self:expr, $f:expr) => {{
        if $self.null_count() == 0 {
            $self.into_no_null_iter().map($f).collect()
        } else {
            $self.downcast_iter().map(|opt_v| opt_v.map($f)).collect()
        }
    }};
}

macro_rules! impl_cmp_numeric_utf8 {
    ($self:ident, $rhs:ident, $op:ident, $kop:ident, $operand:tt) => {{
        // broadcast
        if $rhs.len() == 1 {
            if let Some(value) = $rhs.get(0) {
                $self.$op(value)
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else if $self.len() == 1 {
            if let Some(value) = $self.get(0) {
                let f = |c| value $operand c;
                Ok(apply! {$rhs, f})
            } else {
                Ok(DFBooleanArray::full(false, $rhs.len()))
            }
        } else if $self.len() == $rhs.len() {
            $self.comparison($rhs, comparison::$kop)
        } else {
            Ok(apply_operand_on_array_by_iter!($self, $rhs, $operand))
        }
    }};
}

impl<T> ArrayCompare<&DataArray<T>> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumComp,
{
    fn eq_missing(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        Ok(impl_eq_missing!(self, rhs))
    }

    fn eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, eq, eq,  ==}
    }

    fn neq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, neq, neq,!=}
    }

    fn gt(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, gt,gt, >}
    }

    fn gt_eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, gt_eq, gt_eq, >=}
    }

    fn lt(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, lt, lt,  <}
    }

    fn lt_eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, lt_eq, lt_eq, <=}
    }
}

macro_rules! impl_cmp_bool {
    ($self:ident, $rhs:ident, $operand:tt) => {{
        // broadcast
        if $rhs.len() == 1 {
            if let Some(value) = $rhs.get(0) {
                match value {
                    true => Ok($self.clone()),
                    false => $self.not(),
                }
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else if $self.len() == 1 {
            if let Some(value) = $self.get(0) {
                match value {
                    true => Ok($rhs.clone()),
                    false => $rhs.not(),
                }
            } else {
                Ok(DFBooleanArray::full(false, $rhs.len()))
            }
        } else {
            Ok(apply_operand_on_array_by_iter!($self, $rhs, $operand))
        }
    }};
}

impl ArrayCompare<&DFBooleanArray> for DFBooleanArray {
    fn eq_missing(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        Ok(impl_eq_missing!(self, rhs))
    }

    fn eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, == }
    }

    fn neq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, != }
    }

    fn gt(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, > }
    }

    fn gt_eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, >= }
    }

    fn lt(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, < }
    }

    fn lt_eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, <= }
    }
}

impl DFUtf8Array {
    fn comparison(
        &self,
        rhs: &DFUtf8Array,
        operator: impl Fn(
            &StringArray,
            &StringArray,
        ) -> common_arrow::arrow::error::Result<BooleanArray>,
    ) -> Result<DFBooleanArray> {
        let arr = operator(self.downcast_ref(), rhs.downcast_ref())?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }
}

macro_rules! impl_like_utf8 {
    ($self:ident, $rhs:ident, $op:ident, $kop:ident) => {{
        // broadcast
        if $rhs.len() == 1 {
            if let Some(value) = $rhs.get(0) {
                $self.$op(value)
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else if $self.len() == 1 {
            if let Some(value) = $self.get(0) {
                $rhs.$op(value)
            } else {
                Ok(DFBooleanArray::full(false, $rhs.len()))
            }
        } else {
            $self.comparison($rhs, comparison::$kop)
        }
    }};
}

impl ArrayCompare<&DFUtf8Array> for DFUtf8Array {
    fn eq_missing(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        Ok(impl_eq_missing!(self, rhs))
    }

    fn eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, eq, eq_utf8,  ==}
    }

    fn neq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, neq, neq_utf8,  !=}
    }

    fn gt(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, gt, gt_utf8,  >}
    }

    fn gt_eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, gt_eq, gt_eq_utf8,  >=}
    }

    fn lt(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, lt, lt_utf8,  <}
    }

    fn lt_eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, lt_eq, lt_eq_utf8,  <=}
    }

    fn like(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_like_utf8! {self, rhs, like, like_utf8}
    }

    fn nlike(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_like_utf8! {self, rhs, nlike, nlike_utf8}
    }
}

impl ArrayCompare<&DFNullArray> for DFNullArray {}
impl ArrayCompare<&DFBinaryArray> for DFBinaryArray {}
impl ArrayCompare<&DFStructArray> for DFStructArray {}

pub trait NumComp: Num + NumCast + PartialOrd {}

impl NumComp for f32 {}
impl NumComp for f64 {}
impl NumComp for i8 {}
impl NumComp for i16 {}
impl NumComp for i32 {}
impl NumComp for i64 {}
impl NumComp for u8 {}
impl NumComp for u16 {}
impl NumComp for u32 {}
impl NumComp for u64 {}

impl<T, Rhs> ArrayCompare<Rhs> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    Rhs: NumComp + ToPrimitive,
{
    fn eq_missing(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        self.eq(rhs)
    }

    fn eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);
        match rhs {
            Some(v) => {
                let arr = eq_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::from_arrow_array(arr))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn neq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);
        match rhs {
            Some(v) => {
                let arr = neq_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::from_arrow_array(arr))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn gt(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);
        match rhs {
            Some(v) => {
                let arr = gt_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::from_arrow_array(arr))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn gt_eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);

        match rhs {
            Some(v) => {
                let arr = gt_eq_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::from_arrow_array(arr))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn lt(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);

        match rhs {
            Some(v) => {
                let arr = lt_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::from_arrow_array(arr))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn lt_eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);

        match rhs {
            Some(v) => {
                let arr = lt_eq_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::from_arrow_array(arr))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }
}

impl ArrayCompare<&str> for DFUtf8Array {
    fn eq_missing(&self, rhs: &str) -> Result<DFBooleanArray> {
        self.eq(rhs)
    }

    fn eq(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = eq_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }

    fn neq(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = neq_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }

    fn gt(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = gt_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }

    fn gt_eq(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = gt_eq_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }

    fn lt(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = lt_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }

    fn lt_eq(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = lt_eq_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }

    fn like(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = like_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }

    fn nlike(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = nlike_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(arr))
    }
}

macro_rules! impl_cmp_numeric_utf8_list {
    ($self:ident, $rhs:ident, $cmp_method:ident) => {{
        match ($self.null_count(), $rhs.null_count()) {
            (0, 0) => $self
                .into_no_null_iter()
                .zip($rhs.into_no_null_iter())
                .map(|(left, right)| left.$cmp_method(&right))
                .collect(),
            (0, _) => $self
                .into_no_null_iter()
                .zip($rhs.into_iter())
                .map(|(left, opt_right)| opt_right.map(|right| left.$cmp_method(&right)))
                .collect(),
            (_, 0) => $self
                .into_iter()
                .zip($rhs.into_no_null_iter())
                .map(|(opt_left, right)| opt_left.map(|left| left.$cmp_method(&right)))
                .collect(),
            (_, _) => $self
                .into_iter()
                .zip($rhs.into_iter())
                .map(|(opt_left, opt_right)| match (opt_left, opt_right) {
                    (None, None) => None,
                    (None, Some(_)) => None,
                    (Some(_), None) => None,
                    (Some(left), Some(right)) => Some(left.$cmp_method(&right)),
                })
                .collect(),
        }
    }};
}

impl ArrayCompare<&DFListArray> for DFListArray {
    fn eq_missing(&self, rhs: &DFListArray) -> Result<DFBooleanArray> {
        Ok(impl_cmp_numeric_utf8_list!(self, rhs, series_equal_missing))
    }

    fn eq(&self, rhs: &DFListArray) -> Result<DFBooleanArray> {
        Ok(impl_cmp_numeric_utf8_list!(self, rhs, series_equal))
    }

    fn neq(&self, rhs: &DFListArray) -> Result<DFBooleanArray> {
        self.eq(rhs)?.not()
    }
}

// private
pub(crate) trait ArrayEqualElement {
    /// Check if element in self is equal to element in other, assumes same data_types
    ///
    /// # Safety
    ///
    /// No type checks.
    unsafe fn equal_element(&self, _idx_self: usize, _idx_other: usize, _other: &Series) -> bool {
        unimplemented!()
    }
}

impl<T> ArrayEqualElement for DataArray<T>
where
    T: DFNumericType,
    T::Native: PartialEq,
{
    unsafe fn equal_element(&self, idx_self: usize, idx_other: usize, other: &Series) -> bool {
        let ca_other = other.as_ref().as_ref();
        debug_assert!(self.data_type() == other.data_type());
        let ca_other = &*(ca_other as *const DataArray<T>);
        // Should be get and not get_unchecked, because there could be nulls
        self.get(idx_self) == ca_other.get(idx_other)
    }
}

impl ArrayEqualElement for DFBooleanArray {
    unsafe fn equal_element(&self, idx_self: usize, idx_other: usize, other: &Series) -> bool {
        let ca_other = other.as_ref().as_ref();
        debug_assert!(self.data_type() == other.data_type());
        let ca_other = &*(ca_other as *const DFBooleanArray);
        self.get(idx_self) == ca_other.get(idx_other)
    }
}

impl ArrayEqualElement for DFUtf8Array {
    unsafe fn equal_element(&self, idx_self: usize, idx_other: usize, other: &Series) -> bool {
        let ca_other = other.as_ref().as_ref();
        debug_assert!(self.data_type() == other.data_type());
        let ca_other = &*(ca_other as *const DFUtf8Array);
        self.get(idx_self) == ca_other.get(idx_other)
    }
}

impl ArrayEqualElement for DFListArray {}
impl ArrayEqualElement for DFNullArray {}
impl ArrayEqualElement for DFStructArray {}
impl ArrayEqualElement for DFBinaryArray {}
