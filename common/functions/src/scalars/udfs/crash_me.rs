// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct CrashMeFunction {
    display_name: String,
}

impl CrashMeFunction {
    pub fn try_create(display_name: &str, _arguments: Vec<DataField>) -> Result<Box<dyn Function>> {
        Ok(Box::new(CrashMeFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for CrashMeFunction {
    fn name(&self) -> &str {
        "CrashMeFunction"
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn nullable(&self) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        panic!("crash me function");
    }
}

impl fmt::Display for CrashMeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "crashme")
    }
}
