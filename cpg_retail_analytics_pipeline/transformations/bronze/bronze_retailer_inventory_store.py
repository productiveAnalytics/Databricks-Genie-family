from pyspark import pipelines as dp

SOURCE = "crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_inventory_store"


@dp.materialized_view(
    name="bronze_retailer_inventory_store",
    comment="Bronze layer: raw as-is ingestion of harmonized retailer store-level inventory.",
)
def bronze_retailer_inventory_store():
    return spark.read.table(SOURCE)
