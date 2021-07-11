// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::DataType;
use common_exception::Result;
use dyn_clone::DynClone;

pub trait Function: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;

    fn num_arguments(&self) -> usize {
        0
    }

    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function
    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        None
    }

    fn return_type(&self) -> Result<DataType>;
    fn nullable(&self) -> Result<bool>;
    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn>;
}
