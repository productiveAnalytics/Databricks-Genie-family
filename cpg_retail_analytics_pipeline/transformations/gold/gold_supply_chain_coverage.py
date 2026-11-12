from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_supply_chain_coverage",
    comment="Gold layer: DC-to-store supply coverage per product x retailer x month (average DC on-hand vs. store demand, expressed as a fill rate capped at 1.0).",
)
def gold_supply_chain_coverage():
    # Average daily DC on-hand per product x retailer x month, and how many DCs stock the product.
    dc_supply = (
        spark.read.table("fact_daily_dc_inventory")
        .withColumn("month", F.date_trunc("month", F.col("date_key")))
        .groupBy("product_id", "retailer", "month")
        .agg(
            F.round(F.avg("total_on_hand_quantity"), 2).alias("avg_dc_on_hand"),
            F.max("total_on_hand_quantity").alias("peak_dc_on_hand"),
            F.countDistinct("distribution_center_id").alias("dc_count"),
        )
    )

    # Monthly store demand (units sold) per product x retailer x month.
    store_demand = (
        spark.read.table("fact_daily_sales")
        .withColumn("month", F.date_trunc("month", F.col("date_key")))
        .groupBy("product_id", "retailer", "month")
        .agg(
            F.sum("total_sales_quantity").alias("store_demand_units"),
            F.countDistinct("store_id").alias("selling_store_count"),
        )
    )

    # Also include average store-level on-hand for context.
    store_supply = (
        spark.read.table("fact_daily_store_inventory")
        .withColumn("month", F.date_trunc("month", F.col("date_key")))
        .groupBy("product_id", "retailer", "month")
        .agg(F.round(F.avg("total_on_hand_quantity"), 2).alias("avg_store_on_hand"))
    )

    coverage = (
        store_demand.join(dc_supply, ["product_id", "retailer", "month"], "left")
        .join(store_supply, ["product_id", "retailer", "month"], "left")
        .withColumn(
            "coverage_ratio",
            F.when(
                F.col("store_demand_units") > 0,
                F.round(F.col("avg_dc_on_hand") / F.col("store_demand_units"), 4),
            ),
        )
        .withColumn(
            "fill_rate",
            F.when(
                F.col("coverage_ratio").isNotNull(),
                F.least(F.col("coverage_ratio"), F.lit(1.0)),
            ),
        )
        .withColumn(
            "coverage_status",
            F.when(F.col("avg_dc_on_hand").isNull(), F.lit("NO_DC_STOCK"))
            .when(F.col("fill_rate") >= 1.0, F.lit("FULL"))
            .when(F.col("fill_rate") >= 0.5, F.lit("PARTIAL"))
            .otherwise(F.lit("LOW")),
        )
    )

    products = spark.read.table("dim_product").select(
        "product_id", "product", "upc", "supplier"
    )

    return coverage.join(products, "product_id", "left").select(
        "product_id",
        "product",
        "upc",
        "supplier",
        "retailer",
        "month",
        "store_demand_units",
        "selling_store_count",
        "avg_store_on_hand",
        "avg_dc_on_hand",
        "peak_dc_on_hand",
        "dc_count",
        "coverage_ratio",
        "fill_rate",
        "coverage_status",
    )
