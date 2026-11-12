from pyspark import pipelines as dp

# Read-only source schema containing Crisp harmonized CPG retail sample data.
SOURCE = "crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales"


@dp.materialized_view(
    name="bronze_retailer_sales",
    comment="Bronze layer: raw as-is ingestion of harmonized retailer sales (2021-01-01 to 2022-12-31).",
)
def bronze_retailer_sales():
    return spark.read.table(SOURCE)
