use std::path::Path;

use datafusion::{
    logical_expr::ScalarUDF,
    prelude::{ParquetReadOptions, SessionContext},
};
use datafusion_spatial::udfs::{AsText};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    ctx.register_udf(ScalarUDF::from(AsText::new()));

    let path = "data/data-*_wkb.parquet";

    let table_name = Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");
    println!("Table: {table_name}");

    ctx.register_parquet(table_name, path, ParquetReadOptions::default())
        .await?;

    let df = ctx
        .sql(&format!(
            "SELECT col, ST_AsText(geometry) as wkt FROM '{table_name}'"
        ))
        .await?;

    df.show().await?;

    Ok(())
}
