// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::time::Duration;

use common_datavalues::columns::DataColumn;
use common_datavalues::is_numeric;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct SleepFunction {
    display_name: String,
}

impl SleepFunction {
    pub fn try_create(display_name: &str, arguments: Vec<DataField>) -> Result<Box<dyn Function>> {
        if !is_numeric(arguments[0].data_type()) {
            return Err(ErrorCode::BadArguments(format!(
                "Illegal type {} of argument of function {}, expected numeric",
                &arguments[0].data_type(),
                display_name
            )));
        }
        Ok(Box::new(SleepFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for SleepFunction {
    fn name(&self) -> &str {
        "SleepFunction"
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn nullable(&self) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        match &columns[0] {
            DataColumn::Array(_) => Err(ErrorCode::BadArguments(format!(
                "The argument of function {} must be constant.",
                self.display_name
            ))),
            DataColumn::Constant(value, rows) => {
                let seconds = match value {
                    DataValue::UInt8(Some(v)) => Duration::from_secs(*v as u64),
                    DataValue::UInt16(Some(v)) => Duration::from_secs(*v as u64),
                    DataValue::UInt32(Some(v)) => Duration::from_secs(*v as u64),
                    DataValue::UInt64(Some(v)) => Duration::from_secs(*v as u64),
                    DataValue::Int8(Some(v)) if *v > 0 => Duration::from_secs(*v as u64),
                    DataValue::Int16(Some(v)) if *v > 0 => Duration::from_secs(*v as u64),
                    DataValue::Int32(Some(v)) if *v > 0 => Duration::from_secs(*v as u64),
                    DataValue::Int64(Some(v)) if *v > 0 => Duration::from_secs(*v as u64),
                    DataValue::Float32(Some(v)) => Duration::from_secs_f32(*v),
                    DataValue::Float64(Some(v)) => Duration::from_secs_f64(*v),
                    v => {
                        return Err(ErrorCode::BadArguments(format!(
                            "Sleep must be between 0 and 3 seconds. Requested: {}",
                            v
                        )))
                    }
                };

                if seconds.ge(&Duration::from_secs(3)) {
                    return Err(ErrorCode::BadArguments(format!(
                        "The maximum sleep time is 3 seconds. Requested: {:?}",
                        seconds
                    )));
                }

                std::thread::sleep(seconds);
                Ok(DataColumn::Constant(DataValue::UInt8(Some(0)), *rows))
            }
        }
    }
}

impl fmt::Display for SleepFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "sleep")
    }
}
