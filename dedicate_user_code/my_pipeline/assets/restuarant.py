import pandas as pd
from dagster import MetadataValue, OpExecutionContext, asset


@asset(
    group_name="john_code",
    key_prefix="john_code",
)
def restuarant(context: OpExecutionContext) -> pd.DataFrame:
    pois = [
        {"name": "Shake Shack", "city": "New York", "lat": 40.7580, "lon": -73.9855},
        {
            "name": "Burger King",
            "city": "New York",
            "lat": 40.7580,
            "lon": -73.9855,
        },
        {
            "name": "McDonalds",
            "city": "New York",
            "lat": 40.7580,
            "lon": -73.9855,
        },
        {
            "name": "Wendys",
            "city": "New York",
            "lat": 40.7580,
            "lon": -73.9855,
        },
        {"name": "Taco Bell", "city": "New York", "lat": 40.7580, "lon": -73.9855},
        {"name": "Chipotle", "city": "New York", "lat": 40.7580, "lon": -73.9855},
    ]

    df = pd.DataFrame(pois)

    context.log.info(f"Loaded {len(df)} points of interest")
    context.add_output_metadata(
        {"num_pois": len(df), "preview": MetadataValue.md(df.head().to_markdown())}
    )

    return df
