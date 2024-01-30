import pandas as pd
from dagster import MetadataValue, OpExecutionContext, asset


@asset(
    group_name="seven",
    key_prefix="seven",
)
def poi(context: OpExecutionContext) -> pd.DataFrame:
    pois = [
        {"name": "Eiffel Tower", "city": "Paris", "lat": 48.8584, "lon": 2.2945},
        {
            "name": "Empire State Building",
            "city": "New York",
            "lat": 40.7484,
            "lon": -73.9857,
        },
        {
            "name": "Great Wall of China",
            "city": "Beijing",
            "lat": 40.4319,
            "lon": 116.5704,
        },
        {
            "name": "Great Pyramid of Giza",
            "city": "Giza",
            "lat": 29.9792,
            "lon": 31.1344,
        },
        {"name": "Colosseum", "city": "Rome", "lat": 41.8902, "lon": 12.4922},
        {"name": "Taj Mahal", "city": "Agra", "lat": 27.1751, "lon": 78.0421},
    ]

    df = pd.DataFrame(pois)

    context.log.info(f"Loaded {len(df)} points of interest")
    context.add_output_metadata(
        {"num_pois": len(df), "preview": MetadataValue.md(df.head().to_markdown())}
    )

    return df
