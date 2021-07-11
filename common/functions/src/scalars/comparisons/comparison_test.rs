// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::scalars::*;

#[test]
fn test_comparison_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: Series,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let tests = vec![
        Test {
            name: "eq-passed",
            display: "=",
            nullable: false,
            func: ComparisonEqFunction::try_create_func("", Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![false, false, false, true]),
            error: "",
        },
        Test {
            name: "gt-passed",
            display: ">",
            nullable: false,
            func: ComparisonGtFunction::try_create_func("", Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![true, true, false, false]),
            error: "",
        },
        Test {
            name: "gt-eq-passed",
            display: ">=",
            nullable: false,
            func: ComparisonGtEqFunction::try_create_func("", Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![true, true, false, true]),
            error: "",
        },
        Test {
            name: "lt-passed",
            display: "<",
            nullable: false,
            func: ComparisonLtFunction::try_create_func("", Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![false, false, true, false]),
            error: "",
        },
        Test {
            name: "lt-eq-passed",
            display: "<=",
            nullable: false,
            func: ComparisonLtEqFunction::try_create_func("", Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![false, false, true, true]),
            error: "",
        },
        Test {
            name: "not-eq-passed",
            display: "!=",
            nullable: false,
            func: ComparisonNotEqFunction::try_create_func("", Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![true, true, true, false]),
            error: "",
        },
        Test {
            name: "like-passed",
            display: "LIKE",
            nullable: false,
            func: ComparisonLikeFunction::try_create_func("", Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec!["abc", "abd", "abe", "abf"]).into(),
                Series::new(vec!["a%", "_b_", "abe", "a"]).into(),
            ],
            expect: Series::new(vec![true, true, true, false]),
            error: "",
        },
        Test {
            name: "not-like-passed",
            display: "NOT LIKE",
            nullable: false,
            func: ComparisonNotLikeFunction::try_create_func("", Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec!["abc", "abd", "abe", "abf"]).into(),
                Series::new(vec!["a%", "_b_", "abe", "a"]).into(),
            ],
            expect: Series::new(vec![false, false, false, true]),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();
        let func = t.func;
        if let Err(e) = func.eval(&t.columns, rows) {
            assert_eq!(t.error, e.to_string());
        }
        func.eval(&t.columns, rows)?;

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable()?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.eval(&t.columns, rows)?;
        // Type check.
        let expect_type = func.return_type()?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        let cmp = v.to_array()?.eq(&t.expect)?;
        assert!(cmp.all_true());
    }
    Ok(())
}
