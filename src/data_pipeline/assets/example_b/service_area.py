import pandas as pd
from dagster import AssetIn, MetadataValue, OpExecutionContext, asset


@asset(
    group_name="seven",
    key_prefix="seven",
    ins={"poi": AssetIn(key_prefix="seven")},
)
def service_area(context: OpExecutionContext, poi: pd.DataFrame) -> pd.DataFrame:
    df = poi.copy()
    df["service_area"] = "whatever"

    context.log.info(f"Loaded {len(df)} points of interest")
    context.add_output_metadata(
        {"num_pois": len(df), "preview": MetadataValue.md(df.head().to_markdown())}
    )

    return df

