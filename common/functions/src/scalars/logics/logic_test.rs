// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::scalars::*;

#[test]
fn test_logic_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        func_name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let tests = vec![
        Test {
            name: "and-passed",
            func_name: "AndFunction",
            display: "and",
            nullable: false,
            func: LogicAndFunction::try_create_func("".clone(), Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![true, true, true, false]).into(),
                Series::new(vec![true, false, true, true]).into(),
            ],
            expect: Series::new(vec![true, false, true, false]).into(),
            error: "",
        },
        Test {
            name: "or-passed",
            func_name: "OrFunction",
            display: "or",
            nullable: false,
            func: LogicOrFunction::try_create_func("".clone(), Vec::new())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![true, true, true, false]).into(),
                Series::new(vec![true, false, true, true]).into(),
            ],
            expect: Series::new(vec![true, true, true, true]).into(),
            error: "",
        },
        Test {
            name: "not-passed",
            func_name: "NotFunction",
            display: "not",
            nullable: false,
            func: LogicNotFunction::try_create_func("".clone(), Vec::new())?,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec![true, false]).into()],
            expect: Series::new(vec![false, true]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;
        let rows = t.columns[0].len();
        if let Err(e) = func.eval(&t.columns, rows) {
            assert_eq!(t.error, e.to_string());
        }

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
