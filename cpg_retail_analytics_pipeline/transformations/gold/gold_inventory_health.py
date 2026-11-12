from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_inventory_health",
    comment="Gold layer: days of supply and stock status per product x store, based on the latest inventory snapshot and average daily sales.",
)
def gold_inventory_health():
    inv = spark.read.table("fact_daily_store_inventory")

    # Latest observed inventory date per product x store.
    latest_dates = inv.groupBy("product_id", "store_id").agg(
        F.max("date_key").alias("latest_date_key")
    )

    # On-hand quantity as of that latest date (summed across retailers if present).
    current_on_hand = (
        inv.join(latest_dates, ["product_id", "store_id"])
        .where(F.col("date_key") == F.col("latest_date_key"))
        .groupBy("product_id", "store_id", "retailer")
        .agg(F.sum("total_on_hand_quantity").alias("current_on_hand"))
    )

    # Long-run average daily sales per product x store.
    avg_daily_sales = (
        spark.read.table("fact_daily_sales")
        .groupBy("product_id", "store_id")
        .agg(
            F.round(F.avg("total_sales_quantity"), 4).alias("avg_daily_units"),
            F.sum("total_sales_quantity").alias("total_units_sold"),
        )
    )

    health = (
        current_on_hand.join(avg_daily_sales, ["product_id", "store_id"], "left")
        .withColumn(
            "days_of_supply",
            F.when(
                F.col("avg_daily_units") > 0,
                F.round(F.col("current_on_hand") / F.col("avg_daily_units"), 2),
            ),
        )
        .withColumn(
            "stock_status",
            F.when(F.col("current_on_hand") <= 0, F.lit("OUT_OF_STOCK"))
            .when(
                F.col("avg_daily_units").isNull() | (F.col("avg_daily_units") <= 0),
                F.lit("NO_SALES_HISTORY"),
            )
            .when(F.col("days_of_supply") < 7, F.lit("LOW_STOCK"))
            .when(F.col("days_of_supply") < 30, F.lit("HEALTHY"))
            .otherwise(F.lit("OVERSTOCK")),
        )
    )

    products = spark.read.table("dim_product").select(
        "product_id", "product", "upc", "supplier"
    )
    stores = spark.read.table("dim_store").select(
        "store_id", "store", "store_number", "store_city", "store_state"
    )

    return (
        health.join(products, "product_id", "left")
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
            "current_on_hand",
            "avg_daily_units",
            "total_units_sold",
            "days_of_supply",
            "stock_status",
        )
    )
