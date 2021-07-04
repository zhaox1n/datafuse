// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::scalars::ArithmeticDivFunction;
use crate::scalars::ArithmeticMinusFunction;
use crate::scalars::ArithmeticModuloFunction;
use crate::scalars::ArithmeticMulFunction;
use crate::scalars::ArithmeticPlusFunction;
use crate::scalars::FactoryFuncRef;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ArithmeticFunction {
    op: DataValueArithmeticOperator,
    return_type: DataType,
}

impl ArithmeticFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("+", ArithmeticPlusFunction::try_create_func);
        map.insert("plus", ArithmeticPlusFunction::try_create_func);
        map.insert("-", ArithmeticMinusFunction::try_create_func);
        map.insert("minus", ArithmeticMinusFunction::try_create_func);
        map.insert("*", ArithmeticMulFunction::try_create_func);
        map.insert("multiply", ArithmeticMulFunction::try_create_func);
        map.insert("/", ArithmeticDivFunction::try_create_func);
        map.insert("divide", ArithmeticDivFunction::try_create_func);
        map.insert("%", ArithmeticModuloFunction::try_create_func);
        map.insert("modulo", ArithmeticModuloFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(
        op: DataValueArithmeticOperator,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        let return_type = if arguments.len() == 1 {
            arguments[0].data_type().clone()
        } else {
            common_datavalues::numerical_arithmetic_coercion(
                &op,
                arguments[0].data_type(),
                arguments[1].data_type(),
            )?
        };
        Ok(Box::new(ArithmeticFunction { op, return_type }))
    }
}

impl Function for ArithmeticFunction {
    fn name(&self) -> &str {
        "ArithmeticFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        match columns.len() {
            1 => std::ops::Neg::neg(&columns[0]),
            _ => columns[0].arithmetic(self.op.clone(), &columns[1]),
        }
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 2))
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
