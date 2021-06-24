// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::hashes::siphash::SipHashFunction;
use crate::FactoryFuncRef;
use std::hash::{Hasher, BuildHasher};

#[derive(Clone)]
pub struct HashesFunction;

impl HashesFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("siphash", SipHashFunction::try_create);
        Ok(())
    }
}

pub struct IdHasher {
    hash: u64,
}

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("IdHasher should only be used for u64 keys")
    }

    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }
}

#[derive(Debug)]
pub struct IdHashBuilder {}

impl BuildHasher for IdHashBuilder {
    type Hasher = IdHasher;

    fn build_hasher(&self) -> Self::Hasher {
        IdHasher { hash: 0 }
    }
}

