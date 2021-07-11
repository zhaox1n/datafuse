// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::scalars::*;

#[test]
fn test_to_type_name_function() -> Result<()> {
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

    let tests = vec![Test {
        name: "to_type_name-example-passed",
        display: "toTypeName",
        nullable: false,
        arg_names: vec!["a"],
        func: ToTypeNameFunction::try_create("toTypeName", Vec::default())?,
        columns: vec![Series::new(vec![true, true, true, false]).into()],
        expect: Series::new(vec!["Boolean", "Boolean", "Boolean", "Boolean"]).into(),
        error: "",
    }];

    for t in tests {
        let rows = t.columns[0].len();

        let func = t.func;
        println!("{:?}", t.name);
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

        assert!(v.to_array()?.series_equal(&t.expect.to_array()?));
    }
    Ok(())
}
