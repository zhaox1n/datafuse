// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow;
use common_exception::Result;

use crate::prelude::*;
use crate::DFBooleanArray;

pub struct DataArrayFilter;

impl DataArrayFilter {

    pub fn filter_count(filter: &DFBooleanArray) -> usize {
        let values = filter.downcast_ref().values();
        values.count_set_bits()
    }

    pub fn filter(column: Series, predicate: &DFBooleanArray) -> Result<Series> {
        if predicate.null_count() > 0 {
            // this greatly simplifies subsequent filtering code
            // now we only have a boolean mask to deal with
            let predicate = arrow::compute::prep_null_mask_filter(predicate.downcast_ref());
            return Self::filter(column, &predicate);
        }
        let filter_count = Self::filter_count(DFBooleanArray);
        match filter_count {
            0 => {
                // return empty
                Ok(arrow::array::new_empty_array(array.data_type()).into_series())
            }
            len if len == array.len() => {
                // return all
                let data = array.data().clone();
                Ok(arrow::array::make_array(data).into_series())
            }
            _ => {
                // actually filter
                let data = array.data().clone();
                Ok(arrow::array::make_array(data).into_series())
            }
        }

    }

    pub fn filter_batch_array(
        array: Vec<Series>,
        predicate: &DFBooleanArray,
    ) -> Result<Vec<Series>> {
        if predicate.null_count() > 0 {
            // this greatly simplifies subsequent filtering code
            // now we only have a boolean mask to deal with
            let predicate = arrow::compute::prep_null_mask_filter(predicate.downcast_ref());
            let predicate_array = DFBooleanArray::from_arrow_array(predicate);
            return Self::filter_batch_array(array, &predicate_array);
        }

        let filter = arrow::compute::build_filter(predicate.downcast_ref())?;
        let filtered_arrays = array
            .iter()
            .map(|a| arrow::array::make_array(filter(a.get_array_ref().data())).into_series())
            .collect();
        Ok(filtered_arrays)
    }
}
