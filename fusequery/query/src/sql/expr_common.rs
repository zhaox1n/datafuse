// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ExpressionVisitor;
use common_planners::Recursion;

/// Resolves an `Expression::Wildcard` to a collection of `Expression::Column`'s.
pub fn expand_wildcard(expr: &Expression, schema: &DataSchemaRef) -> Vec<Expression> {
    match expr {
        Expression::Wildcard => schema
            .fields()
            .iter()
            .map(|f| Expression::Column(f.name().to_string()))
            .collect::<Vec<Expression>>(),
        _ => vec![expr.clone()],
    }
}

/// Collect all deeply nested `Expression::AggregateFunction` and
/// `Expression::AggregateUDF`. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_aggregate_exprs(exprs: &[Expression]) -> Vec<Expression> {
    find_exprs_in_exprs(exprs, &|nest_exprs| {
        matches!(nest_exprs, Expression::AggregateFunction { .. })
    })
}

/// Collect all deeply nested `Expression::WindowFunction`
pub fn find_window_exprs(exprs: &[Expression]) -> Vec<Expression> {
    find_exprs_in_exprs(exprs, &|nest_exprs| {
        matches!(nest_exprs, Expression::WindowFunction { .. })
    })
}
/// Collect all arguments from aggregation function and append to this exprs
/// [ColumnExpr(b), Aggr(sum(a, b))] ---> [ColumnExpr(b), ColumnExpr(a)]

pub fn expand_aggregate_arg_exprs(exprs: &[Expression]) -> Vec<Expression> {
    let mut res = vec![];
    for expr in exprs {
        match expr {
            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    if !res.contains(arg) {
                        res.push(arg.clone());
                    }
                }
            }
            _ => {
                if !res.contains(expr) {
                    res.push(expr.clone());
                }
            }
        }
    }
    res
}

pub fn expand_window_exprs(exprs: &Expression) -> (Vec<Expression>, Vec<Expression>) {
    let mut res = vec![];
    let mut sort = vec![];
    match exprs {
        Expression::WindowFunction {args, partition_by, order_by, .. } => {
            let expression_before_windows = args.iter()
                .chain(partition_by.iter())
                .chain(order_by.iter())
                .clone()
                .collect::<Vec<_>>();
            for expr in expression_before_windows {
                if !res.contains(expr) {
                    res.push(expr.clone());
                }
            }
            let sort_expression = partition_by.iter()
                .chain(order_by.iter())
                .clone().collect::<Vec<_>>();
            for expr in sort_expression {
                if !sort.contains(expr) {
                    sort.push(expr.clone())
                }
            }
        }
        _ => {}
    }
    (res, sort)
}

/// Collect all deeply nested `Expression::Column`'s. They are returned in order of
/// appearance (depth first), with duplicates omitted.
pub fn find_column_exprs(exprs: &[Expression]) -> Vec<Expression> {
    find_exprs_in_exprs(exprs, &|nest_exprs| {
        matches!(nest_exprs, Expression::Column(_))
    })
}

/// Search the provided `Expression`'s, and all of their nested `Expression`, for any that
/// pass the provided test. The returned `Expression`'s are deduplicated and returned
/// in order of appearance (depth first).
fn find_exprs_in_exprs<F>(exprs: &[Expression], test_fn: &F) -> Vec<Expression>
where F: Fn(&Expression) -> bool {
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

// Visitor that find Expressionessions that match a particular predicate
struct Finder<'a, F>
where F: Fn(&Expression) -> bool
{
    test_fn: &'a F,
    exprs: Vec<Expression>,
}

impl<'a, F> Finder<'a, F>
where F: Fn(&Expression) -> bool
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            test_fn,
            exprs: Vec::new(),
        }
    }
}

impl<'a, F> ExpressionVisitor for Finder<'a, F>
where F: Fn(&Expression) -> bool
{
    fn pre_visit(mut self, expr: &Expression) -> Result<Recursion<Self>> {
        if (self.test_fn)(expr) {
            if !(self.exprs.contains(expr)) {
                self.exprs.push(expr.clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

/// Search an `Expression`, and all of its nested `Expression`'s, for any that pass the
/// provided test. The returned `Expression`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &Expression, test_fn: &F) -> Vec<Expression>
where F: Fn(&Expression) -> bool {
    let Finder { exprs, .. } = expr
        .accept(Finder::new(test_fn))
        // pre_visit always returns OK, so this will always too
        .expect("no way to return error during recursion");

    exprs
}

/// Convert any `Expression` to an `Expression::Column`.
pub fn expr_as_column_expr(expr: &Expression) -> Result<Expression> {
    match expr {
        Expression::Column(_) => Ok(expr.clone()),
        _ => Ok(Expression::Column(expr.column_name())),
    }
}

/// Rebuilds an `expr` as a projection on top of a collection of `Expression`'s.
///
/// For example, the Expressionession `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base exprs already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
pub fn rebase_expr(expr: &Expression, base_exprs: &[Expression]) -> Result<Expression> {
    clone_with_replacement(expr, &|nest_exprs| {
        if base_exprs.contains(nest_exprs) {
            Ok(Some(expr_as_column_expr(nest_exprs)?))
        } else {
            Ok(None)
        }
    })
}

// Rebuilds an `expr` to ColumnExpr when some expressions already processed in upstream
// Skip Sort, Alias because we can go into the inner nest_exprs
pub fn rebase_expr_from_input(expr: &Expression, schema: &DataSchemaRef) -> Result<Expression> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        Expression::Sort { .. } | Expression::Column(_) | Expression::Alias(_, _) => Ok(None),
        _ => {
            if schema.field_with_name(&nest_exprs.column_name()).is_ok() {
                Ok(Some(expr_as_column_expr(nest_exprs)?))
            } else {
                Ok(None)
            }
        }
    })
}

pub fn sort_to_inner_expr(expr: &Expression) -> Expression {
    match expr {
        Expression::Sort {
            expr: nest_exprs, ..
        } => *nest_exprs.clone(),
        _ => expr.clone(),
    }
}

/// Determines if the set of `Expression`'s are a valid projection on the input
/// `Expression::Column`'s.
pub fn find_columns_not_satisfy_exprs(
    columns: &[Expression],
    exprs: &[Expression],
) -> Result<Option<Expression>> {
    columns.iter().try_for_each(|c| match c {
        Expression::Column(_) => Ok(()),

        _ => Err(ErrorCode::SyntaxException(
            "Expression::Column are required".to_string(),
        )),
    })?;

    let exprs = find_column_exprs(exprs);
    for expr in &exprs {
        if !columns.contains(expr) {
            return Ok(Some(expr.clone()));
        }
    }
    Ok(None)
}

/// Returns a cloned `expr`, but any of the `expr`'s in the tree may be
/// replaced/customized by the replacement function.
///
/// The replacement function is called repeatedly with `expr`, starting with
/// the argument `expr`, then descending depth-first through its
/// descendants. The function chooses to replace or keep (clone) each `expr`.
///
/// The function's return type is `Result<Option<Expression>>>`, where:
///
/// * `Ok(Some(replacement_expr))`: A replacement `expr` is provided; it is
///       swapped in at the particular node in the tree. Any nested `expr` are
///       not subject to cloning/replacement.
/// * `Ok(None)`: A replacement `expr` is not provided. The `expr` is
///       recreated, with all of its nested `expr`'s subject to
///       cloning/replacement.
/// * `Err(err)`: Any error returned by the function is returned as-is by
///       `clone_with_replacement()`.
fn clone_with_replacement<F>(expr: &Expression, replacement_fn: &F) -> Result<Expression>
where F: Fn(&Expression) -> Result<Option<Expression>> {
    let replacement_opt = replacement_fn(expr)?;

    match replacement_opt {
        // If we were provided a replacement, use the replacement. Do not
        // descend further.
        Some(replacement) => Ok(replacement),
        // No replacement was provided, clone the node and recursively call
        // clone_with_replacement() on any nested Expressionessions.
        None => match expr {
            Expression::Wildcard => Ok(Expression::Wildcard),
            Expression::Alias(alias_name, nested_expr) => Ok(Expression::Alias(
                alias_name.clone(),
                Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
            )),

            Expression::UnaryExpression {
                op,
                expr: nested_expr,
            } => Ok(Expression::UnaryExpression {
                op: op.clone(),
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
            }),

            Expression::BinaryExpression { left, op, right } => Ok(Expression::BinaryExpression {
                left: Box::new(clone_with_replacement(&**left, replacement_fn)?),
                op: op.clone(),
                right: Box::new(clone_with_replacement(&**right, replacement_fn)?),
            }),

            Expression::ScalarFunction { op, args } => Ok(Expression::ScalarFunction {
                op: op.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expression>>>()?,
            }),

            Expression::AggregateFunction { op, distinct, args } => {
                Ok(Expression::AggregateFunction {
                    op: op.clone(),
                    distinct: *distinct,
                    args: args
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expression>>>()?,
                })
            }

            Expression::WindowFunction { .. } => Ok(expr.clone()),

            Expression::Sort {
                expr: nested_expr,
                asc,
                nulls_first,
            } => Ok(Expression::Sort {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                asc: *asc,
                nulls_first: *nulls_first,
            }),

            Expression::Cast {
                expr: nested_expr,
                data_type,
            } => Ok(Expression::Cast {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                data_type: data_type.clone(),
            }),

            Expression::Column(_) | Expression::Literal(_) => Ok(expr.clone()),
        },
    }
}

/// Returns mapping of each alias (`String`) to the exprs (`Expression`) it is
/// aliasing.
pub fn extract_aliases(exprs: &[Expression]) -> HashMap<String, Expression> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            Expression::Alias(alias_name, nest_exprs) => {
                Some((alias_name.clone(), *nest_exprs.clone()))
            }
            _ => None,
        })
        .collect::<HashMap<String, Expression>>()
}

/// Rebuilds an `expr` with columns that refer to aliases replaced by the
/// alias' underlying `expr`.
pub fn resolve_aliases_to_exprs(
    expr: &Expression,
    aliases: &HashMap<String, Expression>,
) -> Result<Expression> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        Expression::Column(name) => {
            if let Some(aliased_expr) = aliases.get(name) {
                Ok(Some(aliased_expr.clone()))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    })
}

/// Rebuilds an `expr` using the inner expr for expression
///  `(a + b) as c` ---> `(a + b)`
pub fn unwrap_alias_exprs(expr: &Expression) -> Result<Expression> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        Expression::Alias(_, nested_expr) => Ok(Some(*nested_expr.clone())),
        _ => Ok(None),
    })
}
