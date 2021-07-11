// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct UdfExampleFunction {
    display_name: String,
}

impl UdfExampleFunction {
    pub fn try_create(display_name: &str, _arguments: Vec<DataField>) -> Result<Box<dyn Function>> {
        Ok(Box::new(UdfExampleFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for UdfExampleFunction {
    fn name(&self) -> &str {
        "UdfExampleFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        Ok(DataColumn::Constant(
            DataValue::Boolean(Some(true)),
            input_rows,
        ))
    }

    fn num_arguments(&self) -> usize {
        0
    }
}

impl fmt::Display for UdfExampleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
