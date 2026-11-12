from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="fact_daily_store_inventory",
    comment="Silver layer: daily store inventory facts aggregated by date x product x store x retailer.",
)
def fact_daily_store_inventory():
    return (
        spark.read.table("bronze_retailer_inventory_store")
        .groupBy("date_key", "product_id", "store_id", "retailer")
        .agg(
            F.sum("on_hand_quantity").alias("total_on_hand_quantity"),
            F.countDistinct("inventory_id").alias("inventory_record_count"),
        )
    )
