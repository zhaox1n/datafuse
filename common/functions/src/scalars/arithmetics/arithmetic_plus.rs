// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataField;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::scalars::ArithmeticFunction;
use crate::scalars::Function;

pub struct ArithmeticPlusFunction;

impl ArithmeticPlusFunction {
    pub fn try_create_func(
        _display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus, arguments)
    }
}
