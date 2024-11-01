use std::{path::Path, sync::Arc};

use datafusion::{
    error::Result,
    logical_expr::{AggregateUDF, ScalarUDF},
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};

use datafusion_spatial::{
    rules::SpatialAnalyzerRule,
    udafs::Extent,
    udfs::{AsText, Envelope, GeometryType},
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.skip_metadata = false;
    let ctx = SessionContext::new_with_config(config);

    ctx.register_udf(ScalarUDF::from(AsText::new()));
    ctx.register_udf(ScalarUDF::from(GeometryType::new()));
    ctx.register_udf(ScalarUDF::from(Envelope::new()));

    ctx.register_udaf(AggregateUDF::from(Extent::new()));

    ctx.add_analyzer_rule(Arc::new(SpatialAnalyzerRule {}));

    for path in std::fs::read_dir(Path::new("data/")).unwrap() {
        let path = path.unwrap().path();

        let path_str = path.to_str().unwrap();
        if !path_str.ends_with(".parquet") || path_str.contains("wkb") {
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

        let query = format!("SELECT ST_Envelope(geometry), ST_AsText(geometry) FROM '{}'", table_name);
        let df = ctx.sql(&query).await?;

        df.show_limit(5).await?;

        let query = format!("SELECT ST_Extent(geometry) FROM '{}'", table_name);
        let df = ctx.sql(&query).await?;

        // println!("{}", df.logical_plan().display());
        // println!("{}", df.logical_plan().display_graphviz());

        df.show_limit(5).await?;
    }

    Ok(())
}
