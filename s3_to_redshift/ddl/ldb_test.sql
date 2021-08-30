CREATE TABLE IF NOT EXISTS test.LDB_test
( 
  "sku" INT ENCODE ZSTD NOT NULL,
  "product_name" VARCHAR(200) ENCODE ZSTD,
  "price" FLOAT(2) ENCODE ZSTD
  
);
GRANT SELECT ON test.LDB_test TO "looker";
GRANT SELECT ON test.LDB_test TO "datamodeling";