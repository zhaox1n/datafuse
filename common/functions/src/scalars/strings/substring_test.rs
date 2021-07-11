// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::scalars::Function;
use crate::scalars::SubstringFunction;

#[test]
fn test_substring_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let data_fields = vec![
        DataField::new("a", DataType::Utf8, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::UInt64, false),
    ];

    let tests = vec![
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING",
            nullable: false,
            arg_names: vec!["a", "b", "c"],
            columns: vec![
                Series::new(vec!["abcde"]).into(),
                Series::new(vec![2_i64]).into(),
                Series::new(vec![3_u64]).into(),
            ],
            func: SubstringFunction::try_create("substring", data_fields.clone())?,
            expect: Series::new(vec!["bcd"]).into(),
            error: "",
        },
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING",
            nullable: false,
            arg_names: vec!["a", "b", "c"],
            columns: vec![
                Series::new(vec!["abcde"]).into(),
                Series::new(vec![1_i64]).into(),
                Series::new(vec![3_u64]).into(),
            ],
            func: SubstringFunction::try_create("substring", data_fields.clone())?,
            expect: Series::new(vec!["abc"]).into(),
            error: "",
        },
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING",
            nullable: false,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec!["abcde"]).into(),
                Series::new(vec![2_i64]).into(),
            ],

            func: SubstringFunction::try_create("substring", data_fields.clone())?,
            expect: Series::new(vec!["bcde"]).into(),
            error: "",
        },
        Test {
            name: "substring-1234567890-passed",
            display: "SUBSTRING",
            nullable: false,
            arg_names: vec!["a", "b", "c"],
            columns: vec![
                Series::new(vec!["1234567890"]).into(),
                Series::new(vec![-3_i64]).into(),
                Series::new(vec![3_u64]).into(),
            ],

            func: SubstringFunction::try_create("substring", data_fields.clone())?,
            expect: Series::new(vec!["890"]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;
        let rows = t.columns[0].len();
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
        assert_eq!(v, &t.expect);
    }
    Ok(())
}
