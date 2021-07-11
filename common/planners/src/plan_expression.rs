// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunction;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::scalars::FunctionFactory;
use lazy_static::lazy_static;

use crate::PlanNode;

lazy_static! {
    static ref OP_SET: HashSet<&'static str> = ["database", "version",].iter().copied().collect();
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct ExpressionPlan {
    pub exprs: Vec<Expression>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>,
    pub desc: String,
}

impl ExpressionPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum Expression {
    /// An expression with a alias name.
    Alias(String, Box<Expression>),
    /// Column name.
    Column(String),
    /// Constant value.
    Literal(DataValue),
    /// select * from t where xxx and exists (subquery)
    Exists(Arc<PlanNode>),
    /// A unary expression such as "NOT foo"
    UnaryExpression { op: String, expr: Box<Expression> },

    /// A binary expression such as "age > 40"
    BinaryExpression {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },

    /// ScalarFunction with a set of arguments.
    /// Note: BinaryFunction is a also kind of functions function
    ScalarFunction { op: String, args: Vec<Expression> },

    /// AggregateFunction with a set of arguments.
    AggregateFunction {
        op: String,
        distinct: bool,
        args: Vec<Expression>,
    },

    /// A sort expression, that can be used to sort values.
    Sort {
        /// The expression to sort on
        expr: Box<Expression>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool,
    },
    /// All fields(*) in a schema.
    Wildcard,
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<Expression>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
}

impl Expression {
    pub fn column_name(&self) -> String {
        match self {
            Expression::Alias(name, _expr) => name.clone(),
            Expression::ScalarFunction { op, .. } => {
                match OP_SET.get(&op.to_lowercase().as_ref()) {
                    Some(_) => format!("{}()", op),
                    None => format!("{:?}", self),
                }
            }
            _ => format!("{:?}", self),
        }
    }

    pub fn to_data_field(&self, input_schema: &DataSchemaRef) -> Result<DataField> {
        let name = self.column_name();
        self.to_data_type(input_schema).and_then(|return_type| {
            self.nullable(input_schema)
                .map(|nullable| DataField::new(&name, return_type, nullable))
        })
    }

    pub fn nullable(&self, input_schema: &DataSchemaRef) -> Result<bool> {
        match self {
            Expression::Alias(_, expr) => expr.nullable(input_schema),
            Expression::Column(s) => Ok(input_schema.field_with_name(s)?.is_nullable()),
            Expression::Literal(v) => Ok(v.is_null()),
            Expression::Exists(_) => Ok(false),
            Expression::BinaryExpression { op, left, right } => {
                let arg_fields = vec![
                    left.to_data_field(input_schema)?,
                    right.to_data_field(input_schema)?,
                ];
                let func = FunctionFactory::get(op, arg_fields)?;
                func.nullable()
            }
            Expression::UnaryExpression { op, expr } => {
                let arg_fields = vec![expr.to_data_field(input_schema)?];
                let func = FunctionFactory::get(op, arg_fields)?;
                func.nullable()
            }
            Expression::ScalarFunction { op, args } => {
                let mut arg_fields = Vec::with_capacity(args.len());
                for arg in args {
                    arg_fields.push(arg.to_data_field(input_schema)?);
                }
                let func = FunctionFactory::get(op, arg_fields)?;
                func.nullable()
            }
            Expression::AggregateFunction { .. } => {
                let func = self.to_aggregate_function(input_schema)?;
                func.nullable()
            }
            Expression::Wildcard => Result::Err(ErrorCode::IllegalDataType(
                "Wildcard expressions are not valid to get nullable",
            )),
            Expression::Cast { expr, .. } => expr.nullable(input_schema),
            Expression::Sort { expr, .. } => expr.nullable(input_schema),
        }
    }

    pub fn to_data_type(&self, input_schema: &DataSchemaRef) -> Result<DataType> {
        match self {
            Expression::Alias(_, expr) => expr.to_data_type(input_schema),
            Expression::Column(s) => Ok(input_schema.field_with_name(s)?.data_type().clone()),
            Expression::Literal(v) => Ok(v.data_type()),
            Expression::Exists(_p) => Ok(DataType::Boolean),
            Expression::BinaryExpression { op, left, right } => {
                let arg_fields = vec![
                    left.to_data_field(input_schema)?,
                    right.to_data_field(input_schema)?,
                ];
                let func = FunctionFactory::get(op, arg_fields)?;
                func.return_type()
            }

            Expression::UnaryExpression { op, expr } => {
                let arg_fields = vec![expr.to_data_field(input_schema)?];
                let func = FunctionFactory::get(op, arg_fields)?;
                func.return_type()
            }

            Expression::ScalarFunction { op, args } => {
                let mut arg_fields = Vec::with_capacity(args.len());
                for arg in args {
                    arg_fields.push(arg.to_data_field(input_schema)?);
                }
                let func = FunctionFactory::get(op, arg_fields)?;
                func.return_type()
            }
            Expression::AggregateFunction { .. } => {
                let func = self.to_aggregate_function(input_schema)?;
                func.return_type()
            }
            Expression::Wildcard => Result::Err(ErrorCode::IllegalDataType(
                "Wildcard expressions are not valid to get return type",
            )),
            Expression::Cast { data_type, .. } => Ok(data_type.clone()),
            Expression::Sort { expr, .. } => expr.to_data_type(input_schema),
        }
    }

    pub fn to_aggregate_function(
        &self,
        schema: &DataSchemaRef,
    ) -> Result<Box<dyn AggregateFunction>> {
        match self {
            Expression::AggregateFunction { op, distinct, args } => {
                let mut func_name = op.clone();
                if *distinct {
                    func_name += "Distinct";
                }

                let mut fields = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    fields.push(arg.to_data_field(schema)?);
                }
                AggregateFunctionFactory::get(&func_name, fields)
            }
            _ => Err(ErrorCode::LogicalError(
                "Expression must be aggregated function",
            )),
        }
    }

    pub fn to_aggregate_function_names(&self) -> Result<Vec<String>> {
        match self {
            Expression::AggregateFunction { args, .. } => {
                let mut names = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    names.push(arg.column_name());
                }
                Ok(names)
            }
            _ => Err(ErrorCode::LogicalError(
                "Expression must be aggregated function",
            )),
        }
    }
}

// Also used as expression column name
impl fmt::Debug for Expression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expression::Alias(alias, v) => write!(f, "{:?} as {:#}", v, alias),
            Expression::Column(ref v) => write!(f, "{:#}", v),
            Expression::Literal(ref v) => write!(f, "{:#}", v),
            Expression::Exists(ref v) => write!(f, "Exists({:?})", v),
            Expression::BinaryExpression { op, left, right } => {
                write!(f, "({:?} {} {:?})", left, op, right,)
            }

            Expression::UnaryExpression { op, expr } => {
                write!(f, "({} {:?})", op, expr)
            }

            Expression::ScalarFunction { op, args } => {
                write!(f, "{}(", op)?;

                for (i, _) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i],)?;
                }
                write!(f, ")")
            }

            Expression::AggregateFunction { op, distinct, args } => {
                write!(f, "{}(", op)?;
                if *distinct {
                    write!(f, "distinct ")?;
                }
                for (i, _) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i],)?;
                }
                write!(f, ")")
            }

            Expression::Sort { expr, .. } => write!(f, "{:?}", expr),
            Expression::Wildcard => write!(f, "*"),
            Expression::Cast { expr, data_type } => {
                write!(f, "cast({:?} as {:?})", expr, data_type)
            }
        }
    }
}
