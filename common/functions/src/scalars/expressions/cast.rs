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
pub struct CastFunction {
    /// The data type to cast to
    cast_type: DataType,
    nullable: bool,
}

impl CastFunction {
    pub fn try_create(cast_type: DataType, arguments: Vec<DataField>) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            cast_type,
            nullable: arguments[0].is_nullable(),
        }))
    }
}

impl Function for CastFunction {
    fn name(&self) -> &str {
        "CastFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self) -> Result<bool> {
        Ok(self.nullable)
    }

    fn eval(&self, columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        let series = columns[0].to_minimal_array()?;
        let column: DataColumn = series.cast_with_type(&self.cast_type)?.into();
        Ok(column.resize_constant(input_rows))
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for CastFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST")
    }
}
