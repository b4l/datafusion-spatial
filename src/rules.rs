use std::collections::HashMap;

use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
        Column,
    },
    config::ConfigOptions,
    error::{DataFusionError, Result},
    logical_expr::{
        expr::{AggregateFunction, ScalarFunction},
        LogicalPlan, TableScan,
    },
    optimizer::AnalyzerRule,
    parquet::errors::ParquetError,
    prelude::{lit, Expr},
};
use geoarrow::io::parquet::metadata::GeoParquetMetadata;

pub struct SpatialAnalyzerRule {}

impl AnalyzerRule for SpatialAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut geometa: HashMap<String, GeoParquetMetadata> = HashMap::new();

        let plan = plan.transform_up(|data| {
            // println!("PLAN: {}\n", data.display());

            let transformed = match &data {
                LogicalPlan::TableScan(TableScan {
                    table_name,
                    source: _,
                    projection: _,
                    projected_schema,
                    filters: _,
                    fetch: _,
                }) => {
                    // extract geo metadata
                    if let Some(metadata) = projected_schema.metadata().get("geo") {
                        if !geometa.contains_key(table_name.table()) {
                            let geo: GeoParquetMetadata =
                                serde_json::from_str(metadata).map_err(|e| {
                                    DataFusionError::ParquetError(ParquetError::General(format!(
                                        "Malformed `geo` metadata: {e}"
                                    )))
                                })?;
                            // println!("GEO: {:#?}\n", &geo);
                            geometa.insert(table_name.table().to_string(), geo);
                        }

                        Transformed::no(data)
                    } else {
                        Transformed {
                            data,
                            transformed: false,
                            tnr: TreeNodeRecursion::Jump,
                        }
                    }
                }
                _ => {
                    // rewrite spatial operations
                    data.map_expressions(|expr| {
                        // println!("EXPR: {}\n", expr);

                        let expr = expr.transform_up(|expr| match &expr {
                            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                                if func.name().starts_with("ST_") {
                                    let name = expr.name_for_alias()?;
                                    let mut args = args.to_owned();
                                    let additions = infer_encoding_and_type(&expr, &geometa)?;
                                    args.extend_from_slice(&additions);
                                    Ok(Transformed::yes(
                                        Expr::ScalarFunction(ScalarFunction {
                                            func: func.clone(),
                                            args,
                                        })
                                        .alias(name),
                                    ))
                                } else {
                                    Ok(Transformed::no(expr))
                                }
                            }
                            Expr::AggregateFunction(AggregateFunction {
                                func,
                                args,
                                distinct,
                                filter,
                                order_by,
                                null_treatment,
                            }) => {
                                if func.name().starts_with("ST_") {
                                    let name = expr.name_for_alias()?;
                                    let additions = infer_encoding_and_type(&expr, &geometa)?;
                                    let mut args = args.to_owned();
                                    args.extend_from_slice(&additions);
                                    Ok(Transformed::yes(
                                        Expr::AggregateFunction(AggregateFunction {
                                            func: func.clone(),
                                            args,
                                            distinct: *distinct,
                                            filter: filter.clone(),
                                            order_by: order_by.clone(),
                                            null_treatment: *null_treatment,
                                        })
                                        .alias(name),
                                    ))
                                } else {
                                    Ok(Transformed::no(expr))
                                }
                            }
                            _ => Ok(Transformed::no(expr)),
                        })?;

                        Ok(expr)
                    })?
                }
            };

            Ok(Transformed::no(transformed.data))
        })?;

        // let plan = plan.data.recompute_schema()?;
        Ok(plan.data)
    }

    fn name(&self) -> &str {
        "spatial-analyzer-rule"
    }
}

fn infer_encoding_and_type(
    expr: &Expr,
    geometa: &HashMap<String, GeoParquetMetadata>,
) -> Result<[Expr; 2]> {
    let mut output: [Expr; 2] = Default::default();

    expr.apply_children(|expr| match &expr {
        Expr::Column(Column { relation, name }) => {
            if let Some(table_reference) = relation {
                if let Some(meta) = geometa.get(table_reference.table()) {
                    if let Some(column) = meta.columns.get(name.as_str()) {
                        let encoding = lit(column.encoding.to_string());
                        let geometry_type = match column.geometry_types.len() {
                            0 => lit("Unknown"),
                            1 => lit(column.geometry_types.iter().next().unwrap().to_string()),
                            2.. => lit("Mixed"),
                        };

                        output = [geometry_type, encoding];

                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        }
        Expr::ScalarFunction(ScalarFunction { func, args: _ }) => {
            if func.name().starts_with("ST_") {
                match func.name() {
                    "ST_Envelope" => output = [lit("Polygon"), lit("polygon")],
                    st => todo!("io mapping for {st}"),
                }
            }
            return Ok(TreeNodeRecursion::Stop);
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;

    Ok(output)
}

// fn map_input_to_output(name: &str, args: &[Expr]) -> Result<[Expr; 2]> {
//     match (name, args) {
//         ("ST_AsText", args) => todo!(),
//         (name, args) => unimplemented!("{name}: {args:?}")
//     }
// }
