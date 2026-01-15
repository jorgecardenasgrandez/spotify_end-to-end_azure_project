import dlt
from pyspark.sql.functions import col, current_timestamp, explode, from_json, schema_of_json

@dlt.table(
    name="book_store_catalog.bronze.bronze_orders",
    comment="Raw orders data ingested from the source system",
    table_properties={"quality":"bronze"}
)
def bronze_orders():
  return (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation","/Volumes/book_store_catalog/landing/operational_data/orders_auto_loader/schema")
    .load("/Volumes/book_store_catalog/landing/operational_data/orders/")
    .select("*", col("_metadata.file_path"), current_timestamp().alias("ingesta_current_timestamp"))
  )


@dlt.table(
    name="book_store_catalog.silver.silver_orders_clean",
    comment="Cleaned orders data ingested from the source system",
    table_properties={"quality":"silver"}
)
def silver_orders_clean():
    return (spark.readStream.table("book_store_catalog.bronze.bronze_orders").select(
            "order_id",
            "customer_id",
            col("order_timestamp").cast("timestamp"),
            "payment_method",
            "items",
            "order_status"
        )
    )