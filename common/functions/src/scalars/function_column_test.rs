// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::*;
use common_datavalues::prelude::*;

use crate::scalars::*;

#[test]
fn test_column_function() -> anyhow::Result<()> {
    let data_field = vec![DataField::new("a", DataType::Boolean, false)];
    let schema = DataSchemaRefExt::create(data_field.clone());
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![
        true, true, true, false,
    ])]);

    // Ok.
    {
        let col = ColumnFunction::try_create("a", data_field.clone())?;
        let columns = vec![block.try_column_by_name("a")?.clone()];
        let _ = col.eval(&columns, block.num_rows())?;
    }

    Ok(())
}
