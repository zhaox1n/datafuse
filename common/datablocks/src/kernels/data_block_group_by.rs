// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use hashbrown::HashMap;

use common_datavalues::{DataValue, DataArrayHashDispatcher, combine_hashes_v2};
use common_exception::Result;

use crate::DataBlock;
use common_functions::SipHasher;
use common_functions::IdHashBuilder;
use std::ops::Deref;

// Table for <group_key, (indices, keys) >
pub type GroupIndicesTable = HashMap<Vec<u8>, (Vec<u32>, Vec<DataValue>), ahash::RandomState>;
// Table for <(group_key, keys, block)>
type GroupBlocksTable = Vec<(Vec<u8>, Vec<DataValue>, DataBlock)>;

pub type VecGroupTable = HashMap<u64, (Vec<u32>, Vec<DataValue>), IdHashBuilder>;
type VecGroupBlockTable = Vec<(u64, Vec<DataValue>, DataBlock)>;

impl DataBlock {
    /// Hash group based on row index then return indices and keys.
    /// For example:
    /// row_idx, A
    /// 0, 1
    /// 1, 2
    /// 2, 3
    /// 3, 4
    /// 4, 5
    ///
    /// grouping by [A%3]
    /// 1)
    /// row_idx, group_key, A
    /// 0, 1, 1
    /// 1, 2, 2
    /// 2, 0, 3
    /// 3, 1, 4
    /// 4, 2, 5
    ///
    /// 2) make indices group(for vector compute)
    /// group_key, indices
    /// 0, [2]
    /// 1, [0, 3]
    /// 2, [1, 4]
    ///
    pub fn group_by_get_indices(
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupIndicesTable> {
        let mut group_indices = GroupIndicesTable::default();

        // 1. Get group by columns.
        let mut group_columns = Vec::with_capacity(column_names.len());
        {
            for col in column_names {
                group_columns.push(block.try_column_by_name(&col)?);
            }
        }

        // 2. Make group with indices.
        {
            let mut group_key_len = 0;
            for col in &group_columns {
                let typ = col.data_type();
                if common_datavalues::is_integer(&typ) {
                    group_key_len += common_datavalues::numeric_byte_size(&typ)?;
                } else {
                    group_key_len += 4;
                }
            }

            let mut group_key = Vec::with_capacity(group_key_len);
            for row in 0..block.num_rows() {
                group_key.clear();

                for col in &group_columns {
                    DataValue::concat_row_to_one_key(col, row, &mut group_key)?;
                }

                match group_indices.get_mut(&group_key) {
                    None => {
                        let mut group_keys = Vec::with_capacity(group_key.len());
                        for col in &group_columns {
                            group_keys.push(DataValue::try_from_column(col, row)?);
                        }
                        group_indices.insert(group_key.clone(), (vec![row as u32], group_keys));
                    }
                    Some((v, _)) => {
                        v.push(row as u32);
                    }
                }
            }
        }

        Ok(group_indices)
    }

    /// Hash group based on row index by column names.
    ///
    /// group_by_get_indices and make blocks.
    pub fn group_by(block: &DataBlock, column_names: &[String]) -> Result<GroupBlocksTable> {
        let group_indices = Self::group_by_get_indices(block, column_names)?;

        let mut group_blocks = GroupBlocksTable::default();
        for (group_key, (group_indices, group_keys)) in group_indices {
            let take_block = DataBlock::block_take_by_indices(&block, &group_indices)?;
            group_blocks.push((group_key, group_keys, take_block));
        }

        Ok(group_blocks)
    }

    fn check_key_equal(first: &Vec<DataValue>, second: &Vec<DataValue>) -> bool {
        for i in 0..first.len() {
            if !first.get(i).unwrap().eq(second.get(i).unwrap()) {
                return false;
            }
        }
        return true;
    }

    pub fn group_by_version(
        block: &DataBlock,
        column_names: &[String],
        hash_group_names: &[String],
    ) -> Result<VecGroupBlockTable> {
        let mut group_indices = VecGroupTable::with_hasher(IdHashBuilder{});

        // 1. Get group by columns.
        let mut group_columns = Vec::with_capacity(column_names.len());
        {
            for col in column_names {
                group_columns.push(block.try_column_by_name(&col)?);
            }
        }

        let mut hash_group_columns = Vec::with_capacity(hash_group_names.len());
        {
            for col in hash_group_names {
                hash_group_columns.push(block.try_column_by_name(&col)?);
            }
        }

        let mut group_key_len = 0;
        for col in &group_columns {
            let typ = col.data_type();
            if common_datavalues::is_integer(&typ) {
                group_key_len += common_datavalues::numeric_byte_size(&typ)?;
            } else {
                group_key_len += 4;
            }
        }

        let hashes = combine_hashes_v2(&group_columns)?;

        // 2. Make group with indices.
        {
            for row in 0..block.num_rows() {
                let key = hashes.get(row).unwrap();
                match group_indices.get_mut(key) {
                    None => {
                        let mut group_keys = Vec::with_capacity(group_key_len);
                        for col in &group_columns {
                            group_keys.push(DataValue::try_from_column(col, row)?);
                        }
                        group_indices.insert(key.clone(), (vec![row as u32], group_keys));
                    }
                    Some((v, _)) => {
                        v.push(row as u32);
                    }
                }
            }
        }

        /*let mut group_keys_columns = Vec::with_capacity(group_columns.get(0).unwrap().len());
        {
            for row in 0..group_columns.get(0).unwrap().len() {
                let mut group_keys = Vec::with_capacity(group_key_len);
                for col in &group_columns {
                    group_keys.push(DataValue::try_from_column(col, row)?);
                }
                group_keys_columns.push(group_keys)
            }
        }*/

        // 3. Make Group block
        let mut group_blocks = VecGroupBlockTable::default();
        {
            for (hash_key, (group_indices, key)) in group_indices {
                //let mut next_keys = 0;
                //let mut check_num = group_indices.len();
                //let mut to_check_vec = vec![false; check_num];
                /*while check_num > 0 {
                    let current_key_index = *(group_indices.get(next_keys).unwrap()) as usize;
                    //let current_key = Box::new(group_keys_columns.get(current_key_index).unwrap());
                    //to_check_vec[next_keys] = true;
                    let mut group_per_indices = Vec::default();
                    for i in next_keys..group_indices.len() {
                        let index = *(group_indices.get(i).unwrap()) as usize;
                        //let index_key = group_keys_columns.get(index).unwrap();
                        /*if Self::check_key_equal(current_key.deref(), index_key) {
                            check_num = check_num - 1;
                            group_per_indices.push(index as u32);
                            to_check_vec[i] = true;
                        } else if !to_check_vec.get(i).unwrap() {
                            next_keys = i;
                        }*/
                        check_num = check_num - 1;
                        group_per_indices.push(index as u32);
                        to_check_vec[i] = true;
                    }
                    let take_block = DataBlock::block_take_by_indices(&block, &group_per_indices)?;
                    group_blocks.push((hash_key, current_key.to_vec(), take_block));
                }*/
                //let take_block = DataBlock::block_take_by_indices(&block, &group_per_indices)?;
                //group_blocks.push((hash_key, key, take_block));
                let take_block = DataBlock::block_take_by_indices(&block, &group_indices)?;
                group_blocks.push((hash_key, key, take_block));
            }
        }

        Ok(group_blocks)
    }
}
