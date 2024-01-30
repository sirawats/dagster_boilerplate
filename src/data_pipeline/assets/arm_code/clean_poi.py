import pandas as pd
from dagster import (
    AssetIn,
    AutoMaterializePolicy,
    MetadataValue,
    OpExecutionContext,
    asset,
)


@asset(
    group_name="seven",
    key_prefix="seven",
    ins={"poi": AssetIn(key_prefix="seven")},
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def clean_poi(context: OpExecutionContext, poi: pd.DataFrame) -> pd.DataFrame:
    df = poi.copy()
    df["is_cleaned"] = True

    context.log.info(f"Loaded {len(df)} points of interest")
    context.add_output_metadata(
        {"num_pois": len(df), "preview": MetadataValue.md(df.head().to_markdown())}
    )
    a = {}
    return df