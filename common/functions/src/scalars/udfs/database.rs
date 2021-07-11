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
pub struct DatabaseFunction {}

// we bind database as first argument in eval
impl DatabaseFunction {
    pub fn try_create(
        _display_name: &str,
        _arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(DatabaseFunction {}))
    }
}

impl Function for DatabaseFunction {
    fn name(&self) -> &str {
        "DatabaseFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        Ok(columns[0].clone())
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for DatabaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "database")
    }
}
