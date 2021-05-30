// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::datatypes::{DataType, TimeUnit};
use std::fmt;
use common_datavalues::{DataSchema, DataColumnarValue};
use common_exception::{Result, ErrorCodes};

use crate::function::IFunction;
use ahash::RandomState;
use std::hash::{BuildHasher, Hasher};

use common_arrow::arrow::array::*;
use std::sync::Arc;

use lazy_static::lazy_static;

lazy_static! {
    static ref RANDOM_STATE: RandomState = RandomState::new();
}


#[derive(Clone)]
pub struct HashFunction {}

impl HashFunction {
    pub fn create(_display_name: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(HashFunction {}))
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

fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

macro_rules! hash_array {
    ($array_type:ident, $column: ident, $f: ident, $hashes: ident, $random_state: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                let mut hasher = $random_state.build_hasher();
                hasher.$f(array.value(i));
                *hash = combine_hashes(hasher.finish(), *hash);
            }
        } else {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                let mut hasher = $random_state.build_hasher();
                if !array.is_null(i) {
                    hasher.$f(array.value(i));
                    *hash = combine_hashes(hasher.finish(), *hash);
                }
            }
        }
    };
}

impl IFunction for HashFunction {
    fn name(&self) -> &str {
        "Hash"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        let mut hashes = vec![0; _input_rows];
        let random_state = &RANDOM_STATE;

        for i in 0..columns.len() {
            let col = columns.get(i).unwrap();
            let col = match col {
                DataColumnarValue::Array(array) => (Ok(array.clone())),
                DataColumnarValue::Constant(v, _) => (v.to_array_with_size(1))
            };
            let col = col?;
            match col.data_type() {
                DataType::UInt8 => {
                    hash_array!(UInt8Array, col, write_u8, hashes, random_state);
                }
                DataType::UInt16 => {
                    hash_array!(UInt16Array, col, write_u16, hashes,  random_state);
                }
                DataType::UInt32 => {
                    hash_array!(UInt32Array, col, write_u32, hashes,  random_state);
                }
                DataType::UInt64 => {
                    hash_array!(UInt64Array, col, write_u64, hashes,  random_state);
                }
                DataType::Int8 => {
                    hash_array!(Int8Array, col, write_i8, hashes,  random_state);
                }
                DataType::Int16 => {
                    hash_array!(Int16Array, col, write_i16, hashes,  random_state);
                }
                DataType::Int32 => {
                    hash_array!(Int32Array, col, write_i32, hashes,  random_state);
                }
                DataType::Int64 => {
                    hash_array!(Int64Array, col, write_i64, hashes,  random_state);
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    hash_array!(
                    TimestampMicrosecondArray,
                    col,
                    write_i64,
                    hashes,
                    random_state
                );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    hash_array!(
                    TimestampNanosecondArray,
                    col,
                    write_i64,
                    hashes,
                    random_state
                );
                }
                DataType::Utf8 => {
                    let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                    for (i, hash) in hashes.iter_mut().enumerate() {
                        let mut hasher = random_state.build_hasher();
                        hasher.write(array.value(i).as_bytes());
                        *hash = combine_hashes(hasher.finish(), *hash);
                    }
                }
                _ => {
                    // This is internal because we should have caught this before.
                    return Result::Err(ErrorCodes::BadDataValueType(
                        format!(
                            "Unsupported key type (dictionary index type not supported creating key) {}",
                            col.data_type(),
                        )
                    ));
                }
            };
        }
        Ok(DataColumnarValue::Array(Arc::new(UInt64Array::from(hashes))))
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        None
    }
}

impl fmt::Display for HashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "database")
    }
}
