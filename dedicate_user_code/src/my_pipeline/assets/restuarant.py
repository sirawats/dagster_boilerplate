import pandas as pd
from dagster import MetadataValue, OpExecutionContext, asset, AssetIn


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
    context.add_output_metadata({"num_pois": len(df), "preview": MetadataValue.md(df.head().to_markdown())})

    return df


# # This Asset doesn't work because depends on another asset in another code-location
# @asset(group_name="john_code", key_prefix="john_code", ins={"poi": AssetIn(key_prefix="seven")})
# def process_poi(context: OpExecutionContext, poi: pd.DataFrame) -> pd.DataFrame:
#     pois = [
#         {"name": "Shake Shack", "city": "New York", "lat": 40.7580, "lon": -73.9855},
#         {
#             "name": "Burger King",
#             "city": "New York",
#             "lat": 40.7580,
#             "lon": -73.9855,
#         },
#         {
#             "name": "McDonalds",
#             "city": "New York",
#             "lat": 40.7580,
#             "lon": -73.9855,
#         },
#         {
#             "name": "Wendys",
#             "city": "New York",
#             "lat": 40.7580,
#             "lon": -73.9855,
#         },
#         {"name": "Taco Bell", "city": "New York", "lat": 40.7580, "lon": -73.9855},
#         {"name": "Chipotle", "city": "New York", "lat": 40.7580, "lon": -73.9855},
#     ]

#     df = pd.DataFrame(pois)

#     context.log.info(f"Loaded {len(df)} points of interest")
#     context.add_output_metadata({"num_pois": len(df), "preview": MetadataValue.md(df.head().to_markdown())})

#     return df
