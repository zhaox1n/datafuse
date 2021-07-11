// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::SipHashFunction;

#[test]
fn test_siphash_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        data_field: Vec<DataField>,
        input_column: DataColumn,
        expect_output_column: DataColumn,
        error: &'static str,
    }

    let tests = vec![
        Test {
            name: "Int8Array siphash",
            data_field: vec![DataField::new("", DataType::Int8, false)],
            input_column: Series::new(vec![1i8, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int16Array siphash",
            data_field: vec![DataField::new("", DataType::Int16, false)],
            input_column: Series::new(vec![1i16, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int32Array siphash",
            data_field: vec![DataField::new("", DataType::Int32, false)],
            input_column: Series::new(vec![1i32, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int64Array siphash",
            data_field: vec![DataField::new("", DataType::Int64, false)],
            input_column: Series::new(vec![1i64, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt8Array siphash",
            data_field: vec![DataField::new("", DataType::UInt8, false)],
            input_column: Series::new(vec![1u8, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt16Array siphash",
            data_field: vec![DataField::new("", DataType::UInt16, false)],
            input_column: Series::new(vec![1u16, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt32Array siphash",
            data_field: vec![DataField::new("", DataType::UInt32, false)],
            input_column: Series::new(vec![1u32, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt64Array siphash",
            data_field: vec![DataField::new("", DataType::UInt64, false)],
            input_column: Series::new(vec![1u64, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Float32Array siphash",
            data_field: vec![DataField::new("", DataType::Float32, false)],
            input_column: Series::new(vec![1.0f32, 2., 1.]).into(),
            expect_output_column: Series::new(vec![
                729488449357906283u64,
                9872512741335963328,
                729488449357906283,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Float64Array siphash",
            data_field: vec![DataField::new("", DataType::Float64, false)],
            input_column: Series::new(vec![1.0f64, 2., 1.]).into(),
            expect_output_column: Series::new(vec![
                13833534234735907638u64,
                12773237290464453619,
                13833534234735907638,
            ])
            .into(),
            error: "",
        },
    ];

    for test in tests {
        let function = SipHashFunction::try_create("siphash", test.data_field)?;

        let rows = test.input_column.len();
        match function.eval(&[test.input_column], rows) {
            Ok(result_column) => assert_eq!(
                &result_column.get_array_ref()?,
                &test.expect_output_column.get_array_ref()?,
                "failed in the test: {}",
                test.name
            ),
            Err(error) => assert_eq!(
                test.error,
                error.to_string(),
                "failed in the test: {}",
                test.name
            ),
        };
    }

    Ok(())
}
