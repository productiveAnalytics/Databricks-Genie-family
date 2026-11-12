from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_sales_velocity",
    comment="Gold layer: average weekly sales rate per product x store (units and revenue per week with sales).",
)
def gold_sales_velocity():
    # Weekly totals per product x store, then average across the weeks that had sales.
    weekly = (
        spark.read.table("fact_daily_sales")
        .withColumn("sales_week", F.date_trunc("week", F.col("date_key")))
        .groupBy("product_id", "store_id", "retailer", "sales_week")
        .agg(
            F.sum("total_sales_quantity").alias("weekly_units"),
            F.sum("total_sales_amount").alias("weekly_revenue"),
        )
    )

    velocity = weekly.groupBy("product_id", "store_id", "retailer").agg(
        F.count("sales_week").alias("weeks_with_sales"),
        F.round(F.avg("weekly_units"), 2).alias("avg_weekly_units"),
        F.round(F.avg("weekly_revenue"), 2).alias("avg_weekly_revenue"),
        F.sum("weekly_units").alias("total_units_sold"),
        F.round(F.sum("weekly_revenue"), 2).alias("total_revenue"),
        F.min("sales_week").alias("first_sale_week"),
        F.max("sales_week").alias("last_sale_week"),
    )

    products = spark.read.table("dim_product").select(
        "product_id", "product", "upc", "supplier"
    )
    stores = spark.read.table("dim_store").select(
        "store_id", "store", "store_number", "store_city", "store_state"
    )

    return (
        velocity.join(products, "product_id", "left")
        .join(stores, "store_id", "left")
        .select(
            "product_id",
            "product",
            "upc",
            "supplier",
            "store_id",
            "store",
            "store_number",
            "store_city",
            "store_state",
            "retailer",
            "weeks_with_sales",
            "avg_weekly_units",
            "avg_weekly_revenue",
            "total_units_sold",
            "total_revenue",
            "first_sale_week",
            "last_sale_week",
        )
    )
