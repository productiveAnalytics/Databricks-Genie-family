from pyspark import pipelines as dp

DC_COLS = [
    "distribution_center_id",
    "distribution_center",
    "distribution_center_key",
    "distribution_center_address",
    "distribution_center_city",
    "distribution_center_state",
    "distribution_center_zip",
    "distribution_center_latitude",
    "distribution_center_longitude",
    "retailer",
    "market_key",
]


@dp.materialized_view(
    name="dim_distribution_center",
    comment="Silver layer: distribution center dimension, one row per distribution_center_id.",
)
def dim_distribution_center():
    return (
        spark.read.table("bronze_retailer_inventory_dc")
        .select(*DC_COLS)
        .dropDuplicates(["distribution_center_id"])
    )
