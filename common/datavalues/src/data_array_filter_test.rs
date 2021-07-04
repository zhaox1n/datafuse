// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::prelude::*;

#[test]
fn filter_batch_array() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use super::*;

    #[allow(dead_code)]
    struct FilterArrayTest {
        name: &'static str,
        filter: DFBooleanArray,
        expect: Vec<Series>,
    }

    let batch_array: Vec<Series> = vec![
        Series::new(vec![1i64, 2, 3, 4, 5]).into(),
        Series::new(vec![6i64, 7, 8, 9, 10]).into(),
    ];

    let tests = vec![
        FilterArrayTest {
            name: "normal filter",
            filter: DFBooleanArray::new_from_slice(&vec![true, false, true, false, true]).into(),
            expect: vec![
                Series::new(vec![1i64, 3, 5]).into(),
                Series::new(vec![6i64, 8, 10]).into(),
            ],
        },
        FilterArrayTest {
            name: "filter contain null",
            filter: DFBooleanArray::new_from_opt_slice(&vec![
                Some(true),
                Some(false),
                Some(true),
                None,
                None,
            ])
            .into(),
            expect: vec![
                Series::new(vec![1i64, 3]).into(),
                Series::new(vec![6i64, 8]).into(),
            ],
        },
    ];

    for t in tests {
        let result = DataArrayFilter::filter_batch_array(batch_array.to_vec(), &t.filter)?;
        assert_eq!(t.expect.len(), result.len());
        for i in 0..t.expect.len() {
            assert_eq!(
                result
                    .get(i)
                    .unwrap()
                    .series_equal(t.expect.get(i).unwrap()),
                true,
                "{}",
                t.name
            )
        }
    }

    Ok(())
}
