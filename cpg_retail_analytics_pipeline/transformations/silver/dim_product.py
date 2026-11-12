from pyspark import pipelines as dp

PRODUCT_COLS = ["product_id", "product", "product_key", "upc", "supplier"]


@dp.materialized_view(
    name="dim_product",
    comment="Silver layer: product dimension, one row per product_id, sourced from sales and inventory bronze tables.",
)
def dim_product():
    sales = spark.read.table("bronze_retailer_sales").select(*PRODUCT_COLS)
    store_inv = spark.read.table("bronze_retailer_inventory_store").select(*PRODUCT_COLS)
    dc_inv = spark.read.table("bronze_retailer_inventory_dc").select(*PRODUCT_COLS)

    return (
        sales.unionByName(store_inv)
        .unionByName(dc_inv)
        .dropDuplicates(["product_id"])
    )
