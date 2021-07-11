// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use indexmap::IndexMap;
use lazy_static::lazy_static;

use crate::scalars::ArithmeticFunction;
use crate::scalars::ComparisonFunction;
use crate::scalars::Function;
use crate::scalars::HashesFunction;
use crate::scalars::LogicFunction;
use crate::scalars::StringFunction;
use crate::scalars::UdfFunction;

pub struct FunctionFactory;
pub type FactoryFunc = fn(name: &str, argument: Vec<DataField>) -> Result<Box<dyn Function>>;

pub type FactoryFuncRef = Arc<RwLock<IndexMap<&'static str, FactoryFunc>>>;

lazy_static! {
    static ref FACTORY: FactoryFuncRef = {
        let map: FactoryFuncRef = Arc::new(RwLock::new(IndexMap::new()));
        ArithmeticFunction::register(map.clone()).unwrap();
        ComparisonFunction::register(map.clone()).unwrap();
        LogicFunction::register(map.clone()).unwrap();
        StringFunction::register(map.clone()).unwrap();
        UdfFunction::register(map.clone()).unwrap();
        HashesFunction::register(map.clone()).unwrap();
        map
    };
}

impl FunctionFactory {
    pub fn get(name: &str, argument: Vec<DataField>) -> Result<Box<dyn Function>> {
        let map = FACTORY.read();
        let creator = map
            .get(&*name.to_lowercase())
            .ok_or_else(|| ErrorCode::UnknownFunction(format!("Unsupported Function: {}", name)))?;
        (creator)(name, argument)
    }

    pub fn check(name: &str) -> bool {
        let map = FACTORY.read();
        map.contains_key(&*name.to_lowercase())
    }

    pub fn registered_names() -> Vec<String> {
        let map = FACTORY.read();
        map.keys().into_iter().map(|x| x.to_string()).collect()
    }
}
