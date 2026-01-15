/*
Proceso para "customer data"
- Ingestar la data de "customers" que se encuentra en la capa landing hacia el lakehouse mediante carga incremental (autoloader) => bronze_customer
- Realizar checks de calidad de datos y transformaciones de data requeridos => silver_customer_clean
- Aplicar cambios a "customer data" => silver_customer
*/

CREATE OR REFRESH STREAMING TABLE book_store_catalog.bronze.bronze_customer
COMMENT 'Raw customer data ingestado desde fuente de sistema dato operacional'
TBLPROPERTIES ('quality'='bronze')
AS
SELECT 
  *,
  _metadata.file_path as input_filepath,
  current_timestamp() as input_current_timestamp
FROM cloud_files(
  '/Volumes/book_store_catalog/landing/operational_data/customers/',
  'json',
  map('cloudFiles.schemaLocation', '/Volumes/book_store_catalog/landing/operational_data/customers_autoloader/schema')
);


CREATE OR REFRESH STREAMING TABLE book_store_catalog.silver.silver_customer_clean(
    CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_customer_name EXPECT (customer_name IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_telephone EXPECT (LENGTH(telephone)>=10),
    CONSTRAINT valid_date_of_birth EXPECT (date_of_birth >= '1920-01-01')
)
COMMENT 'Data cleaned and transformed for customer data'
TBLPROPERTIES ('quality'='silver')
AS
SELECT 
customer_id,
customer_name,
CAST(date_of_birth as DATE) as date_of_birth,
telephone,
email,
CAST(created_date as DATE) as created_date
FROM STREAM(LIVE.book_store_catalog.bronze.bronze_customer);


CREATE OR REFRESH STREAMING TABLE book_store_catalog.silver.silver_customer
COMMENT 'SCD Type 1 customer data'
TBLPROPERTIES ('quality' = 'silver');

APPLY CHANGES INTO book_store_catalog.silver.silver_customer
FROM STREAM(LIVE.book_store_catalog.silver.silver_customer_clean)
KEYS (customer_id)
SEQUENCE BY created_date
STORED AS SCD TYPE 1;