from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="fact_daily_sales",
    comment="Silver layer: daily sales facts aggregated by date x product x store x retailer.",
)
def fact_daily_sales():
    return (
        spark.read.table("bronze_retailer_sales")
        .groupBy("date_key", "product_id", "store_id", "retailer")
        .agg(
            F.sum("sales_quantity").alias("total_sales_quantity"),
            F.sum("sales_amount").alias("total_sales_amount"),
            F.countDistinct("sales_id").alias("sales_record_count"),
        )
    )
