from pyspark import pipelines as dp

STORE_COLS = [
    "store_id",
    "store",
    "store_key",
    "store_uniqueness_key",
    "store_number",
    "store_type",
    "store_address",
    "store_city",
    "store_state",
    "store_zip",
    "store_latitude",
    "store_longitude",
    "retailer",
    "market_key",
]


@dp.materialized_view(
    name="dim_store",
    comment="Silver layer: store dimension, one row per store_id, sourced from sales and store inventory bronze tables.",
)
def dim_store():
    sales = spark.read.table("bronze_retailer_sales").select(*STORE_COLS)
    store_inv = spark.read.table("bronze_retailer_inventory_store").select(*STORE_COLS)

    return sales.unionByName(store_inv).dropDuplicates(["store_id"])
