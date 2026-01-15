from pyspark.sql.functions import current_timestamp, col
import dlt

@dlt.table(
    name="book_store_catalog.bronze.bronze_addresses", 
    comment="Raw addresses data ingested from the source system",
    table_properties={"quality":"bronze"})
def bronze_adds():
    return (spark.readStream.format("cloudFiles").option("cloudFiles.schemaLocation", "/Volumes/book_store_catalog/landing/operational_data/addresses_autoloader/schema").option("cloudFiles.format", "csv").load("/Volumes/book_store_catalog/landing/operational_data/addresses/").select("*", col("_metadata.file_path"), current_timestamp().alias("ingesta_current_timestamp")))


@dlt.table(
    name="book_store_catalog.silver.silver_addresses_clean", 
    comment="Cleaned addresses data ingested from the source system", 
    table_properties={"quality":"silver"}
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_address_line", "address_line_1 IS NOT NULL")
@dlt.expect("valid_postcode", "LENGTH(postcode) <= 5")
def silver_addresses_clean():
    return (spark
            .readStream
            .table("book_store_catalog.bronze.bronze_addresses")
            .select(
                "customer_id",
                "address_line_1",
                "city",
                "state",
                "postcode",
                col("created_date").cast("date")
            )
    )

dlt.create_streaming_table(name="book_store_catalog.silver.silver_addresses",
                           comment="SCD Type 2 addresses data",
                           table_properties={"quality":"silver"})


dlt.apply_changes(
    target="book_store_catalog.silver.silver_addresses",
    source="book_store_catalog.silver.silver_addresses_clean",
    keys=["customer_id"],
    sequence_by="created_date",
    stored_as_scd_type=2
)




