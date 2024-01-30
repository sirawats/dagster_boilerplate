import pandas as pd
from dagster import (
    AssetIn,
    MetadataValue,
    OpExecutionContext,
    asset,
    IdentityPartitionMapping,
    StaticPartitionsDefinition,
)

partitions_def = StaticPartitionsDefinition(["v1", "v2", "v3"])


@asset(
    group_name="seven",
    key_prefix="seven",
    ins={
        "dataset": AssetIn(
            key_prefix="seven",
            partition_mapping=IdentityPartitionMapping(),
            metadata={"allow_missing_partitions": True},
        )
    },
    partitions_def=partitions_def,
)
def run_ml(context: OpExecutionContext, dataset: pd.DataFrame) -> pd.DataFrame:
    df = dataset.copy()
    df["service_area"] = "whatever"

    context.log.info(f"Loaded {len(df)} points of interest")
    context.add_output_metadata(
        {"num_pois": len(df), "preview": MetadataValue.md(df.head().to_markdown())}
    )

    return df
