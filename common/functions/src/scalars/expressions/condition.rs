// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::borrow::Borrow;
use std::convert::TryFrom;
use std::fmt;

use common_datavalues::arrays::PrimitiveArrayBuilder;
use common_datavalues::columns::DataColumn;
use common_datavalues::columns::DataColumn::Array;
use common_datavalues::columns::DataColumn::Constant;
use common_datavalues::prelude::*;
use common_datavalues::DFBooleanArray;
use common_datavalues::DFUInt8Array;
use common_datavalues::DataType;
use common_datavalues::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct ConditionFunction {
    return_type: DataType,
    nullable: bool,
}

impl ConditionFunction {
    pub fn try_create(display_name: &str, arguments: Vec<DataField>) -> Result<Box<dyn Function>> {
        if arguments.len() != 3 {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "{} expect to have 3 argument",
                display_name
            )));
        }
        if arguments[0].data_type() != &DataType::Boolean {
            return Err(ErrorCode::BadArguments(format!(
                "if does not support type {:?}",
                arguments[0].data_type()
            )));
        }
        println!("{:?}", arguments[1].data_type());
        println!("{:?}", arguments[2].data_type());
        if arguments[1].data_type() != arguments[2].data_type() {
            return Err(ErrorCode::BadArguments(
                "The types of parameters should be the same".to_string(),
            ));
        }
        Ok(Box::new(ConditionFunction {
            return_type: arguments[1].data_type().clone(),
            nullable: arguments[1].is_nullable() || arguments[2].is_nullable(),
        }))
    }
}

impl Function for ConditionFunction {
    fn name(&self) -> &str {
        "ConditionFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self) -> Result<bool> {
        Ok(self.nullable)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        let flag_values = columns[0].borrow();
        let true_values = columns[1].borrow();
        let false_values = columns[2].borrow();
        match flag_values {
            Constant(flag, _) => {
                if flag.is_null() || !(bool::try_from(flag.clone())?) {
                    Ok(false_values.clone())
                } else {
                    Ok(true_values.clone())
                }
            }
            Array(flag_values) => {
                let flag_values = flag_values.bool()?;
                let true_values_array = true_values.to_array()?;
                let false_values_array = false_values.to_array()?;
                Ok(
                    Self::if_then_else(flag_values, &true_values_array, &false_values_array)?
                        .into(),
                )
            }
        }
    }

    fn num_arguments(&self) -> usize {
        3
    }
}

impl fmt::Display for ConditionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "if")
    }
}

macro_rules! if_then_else {
    ($BUILDER_TYPE:ident, $ARRAY_TYPE:ident, $BOOLS:expr, $TRUE:expr, $FALSE:expr) => {{
        let mut build = $BUILDER_TYPE::<$ARRAY_TYPE>::new($BOOLS.len());
        for i in 0..$BOOLS.len() {
            if $BOOLS.get(i).unwrap_or(false) {
                build.append_option($TRUE.get(i))
            } else {
                build.append_option($FALSE.get(i));
            }
        }

        Ok(build.finish().into_series())
    }};
}

impl ConditionFunction {
    pub fn if_then_else_u8(
        flag_values: &DFBooleanArray,
        true_values: &DFUInt8Array,
        false_values: &DFUInt8Array,
    ) -> Result<Series> {
        let mut res = PrimitiveArrayBuilder::<UInt8Type>::new(false_values.len());
        for i in 0..flag_values.len() {
            if flag_values.get(i).unwrap_or(false) {
                res.append_option(true_values.get(i))
            } else {
                res.append_option(false_values.get(i));
            }
        }

        Ok(res.finish().into_series())
    }

    pub fn if_then_else(
        flag_values: &DFBooleanArray,
        true_values: &Series,
        false_values: &Series,
    ) -> Result<Series> {
        match true_values.data_type() {
            DataType::Int8 => if_then_else! {
                PrimitiveArrayBuilder, Int8Type, flag_values, true_values.i8()?, false_values.i8()?
            },
            DataType::Int16 => if_then_else! {
                PrimitiveArrayBuilder, Int16Type, flag_values, true_values.i16()?, false_values.i16()?
            },
            DataType::Int32 => if_then_else! {
                PrimitiveArrayBuilder, Int32Type, flag_values, true_values.i32()?, false_values.i32()?
            },
            DataType::Int64 => if_then_else! {
                PrimitiveArrayBuilder, Int64Type, flag_values, true_values.i64()?, false_values.i64()?
            },
            DataType::UInt8 => if_then_else! {
                PrimitiveArrayBuilder, UInt8Type, flag_values, true_values.u8()?, false_values.u8()?
            },
            DataType::UInt16 => if_then_else! {
                PrimitiveArrayBuilder, UInt16Type, flag_values, true_values.u16()?, false_values.u16()?
            },
            DataType::UInt32 => if_then_else! {
                PrimitiveArrayBuilder, UInt32Type, flag_values, true_values.u32()?, false_values.u32()?
            },
            DataType::UInt64 => if_then_else! {
                PrimitiveArrayBuilder, UInt64Type, flag_values, true_values.u64()?, false_values.u64()?
            },
            DataType::Float32 => if_then_else! {
                PrimitiveArrayBuilder, Float32Type, flag_values, true_values.f32()?, false_values.f32()?
            },
            DataType::Float64 => if_then_else! {
                PrimitiveArrayBuilder, Float64Type, flag_values, true_values.f64()?, false_values.f64()?
            },
            /*           DataType::Boolean => if_then_else! {values},
            DataType::Utf8 => if_then_else! {Utf8, values},*/
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{} for DataValue List",
                other,
            ))),
        }
    }
}
