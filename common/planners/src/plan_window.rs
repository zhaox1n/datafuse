// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum FrameType {
    Rows,
    Range,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum FrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<u64>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<u64>),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct WindowFrame {
    pub frame_type: FrameType,
    pub start_bound: FrameBound,
    /// The right bound of the `BETWEEN .. AND` clause. The end bound of `None`
    /// indicates the shorthand form (e.g. `ROWS 1 PRECEDING`), which must
    /// behave the same as `end_bound = WindowFrameBound::CurrentRow`.
    pub end_bound: Option<FrameBound>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct WindowPlan {
    pub func_expr: Expression,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<Expression>,
    pub frame: Option<WindowFrame>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>,
}

impl WindowPlan {
    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
