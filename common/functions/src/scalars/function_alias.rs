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
pub struct AliasFunction {
    alias: String,
    return_type: DataType,
}

impl AliasFunction {
    pub fn try_create(alias: String, arguments: Vec<DataField>) -> Result<Box<dyn Function>> {
        Ok(Box::new(AliasFunction {
            alias,
            return_type: arguments[0].data_type().clone(),
        }))
    }
}

impl Function for AliasFunction {
    fn name(&self) -> &str {
        "AliasFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        Ok(columns[0].clone())
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for AliasFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.alias)
    }
}
