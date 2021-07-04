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

#[derive(Clone, Debug)]
pub struct ColumnFunction {
    value: String,
    saved: Option<DataValue>,
    return_type: DataType,
    nullable: bool,
}

impl ColumnFunction {
    pub fn try_create(value: &str, arguments: Vec<DataField>) -> Result<Box<dyn Function>> {
        Ok(Box::new(ColumnFunction {
            value: value.to_string(),
            saved: None,
            return_type: arguments[0].data_type().clone(),
            nullable: arguments[0].is_nullable(),
        }))
    }
}

impl Function for ColumnFunction {
    fn name(&self) -> &str {
        "ColumnFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self) -> Result<bool> {
        Ok(self.nullable)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        Ok(columns[0].clone())
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for ColumnFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.value)
    }
}
