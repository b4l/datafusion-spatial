use std::{path::Path, sync::Arc};

use datafusion::{
    error::Result,
    logical_expr::ScalarUDF,
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};

use datafusion_spatial::{
    rules::SpatialAnalyzerRule,
    udfs::{AsText, GeometryType},
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.skip_metadata = false;
    let ctx = SessionContext::new_with_config(config);

    ctx.register_udf(ScalarUDF::from(AsText::new()));
    ctx.register_udf(ScalarUDF::from(GeometryType::new()));

    ctx.add_analyzer_rule(Arc::new(SpatialAnalyzerRule {}));

    for path in std::fs::read_dir(Path::new("data/")).unwrap() {
        let path = path.unwrap().path();

        if !path.to_str().unwrap().ends_with(".parquet") {
            println!("{path:?}");
            continue;
        }

        let table_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");
        println!("TABLE: {table_name}\n");

        ctx.register_parquet(
            table_name,
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

        let df = ctx
            .sql(&format!(
                "SELECT col, ST_GeometryType(geometry) as geom_type, ST_AsText(geometry) as wkt FROM '{}'",
                table_name // arrow_typeof(geometry) ST_GeometryType(geometry) as geom_type, ST_AsText(geometry) as wkt
            ))
            .await?;

        // println!("{}", df.logical_plan().display());
        // println!("{}", df.logical_plan().display_graphviz());

        df.show().await?;
    }

    Ok(())
}
