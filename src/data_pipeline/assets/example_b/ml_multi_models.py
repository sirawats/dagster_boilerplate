import pandas as pd
from dagster import (
    AssetIn,
    MetadataValue,
    OpExecutionContext,
    StaticPartitionsDefinition,
    asset,
)

partitions_def = StaticPartitionsDefinition(
    ["random_forest", "linear-regression", "multi-layer-perceptron"]
)


@asset(
    group_name="seven",
    key_prefix="seven",
    ins={"service_area": AssetIn(key_prefix="seven")},
    partitions_def=partitions_def,
)
def run_ml_multi_model(
    context: OpExecutionContext, service_area: pd.DataFrame
) -> pd.DataFrame:
    df = service_area.copy()
    df["service_area"] = "whatever"

    context.log.info(f"Loaded {len(df)} points of interest")
    context.add_output_metadata(
        {"num_pois": len(df), "preview": MetadataValue.md(df.head().to_markdown())}
    )

    return df
