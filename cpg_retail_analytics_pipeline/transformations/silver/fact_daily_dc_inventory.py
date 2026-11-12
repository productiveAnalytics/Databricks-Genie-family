from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="fact_daily_dc_inventory",
    comment="Silver layer: daily distribution center inventory facts aggregated by date x product x DC x retailer.",
)
def fact_daily_dc_inventory():
    return (
        spark.read.table("bronze_retailer_inventory_dc")
        .groupBy("date_key", "product_id", "distribution_center_id", "retailer")
        .agg(
            F.sum("on_hand_quantity").alias("total_on_hand_quantity"),
            F.countDistinct("inventory_id").alias("inventory_record_count"),
        )
    )
