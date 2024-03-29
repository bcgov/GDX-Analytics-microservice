CREATE TABLE IF NOT EXISTS microservice.ldb_sku
( 
"sku" INTEGER ENCODE ZSTD NOT NULL,
"product_name" VARCHAR(200) ENCODE ZSTD,
"image" VARCHAR(200) ENCODE ZSTD,
"body" VARCHAR(4000) ENCODE ZSTD,
"volume" NUMERIC(4,2) ENCODE ZSTD,
"bottles_per_pack" SMALLINT ENCODE ZSTD,
"regular_price" NUMERIC(8,2) ENCODE ZSTD,
"lto_price" NUMERIC(7,2) ENCODE ZSTD,
"lto_start" DATE ENCODE ZSTD,
"lto_end" DATE ENCODE ZSTD,
"price_override" BOOLEAN ENCODE ZSTD,
"store_count" SMALLINT ENCODE ZSTD,
"inventory" INTEGER ENCODE ZSTD,
"availability_override" BOOLEAN ENCODE ZSTD,
"whitelist" BOOLEAN ENCODE ZSTD,
"blacklist" BOOLEAN ENCODE ZSTD,
"upc" VARCHAR(16) ENCODE ZSTD,
"all_upcs" VARCHAR(400) ENCODE ZSTD,
"alcohol" NUMERIC(4,2) ENCODE ZSTD,
"kosher" BOOLEAN ENCODE ZSTD,
"organic" BOOLEAN ENCODE ZSTD,
"sweetness" VARCHAR(2) ENCODE ZSTD,
"vqa" BOOLEAN ENCODE ZSTD,
"craft_beer" BOOLEAN ENCODE ZSTD,
"bcl_select" BOOLEAN ENCODE ZSTD,
"new_flag" BOOLEAN ENCODE ZSTD,
"rating" NUMERIC(2,1) ENCODE ZSTD,
"votes" INTEGER ENCODE ZSTD,
"product_type" VARCHAR(19) ENCODE ZSTD,
"category" VARCHAR(19) ENCODE ZSTD,
"sub_category" VARCHAR(30) ENCODE ZSTD,
"country" VARCHAR(20) ENCODE ZSTD,
"region" VARCHAR(23) ENCODE ZSTD,
"sub_region" VARCHAR(26) ENCODE ZSTD,
"grape_variety" VARCHAR(27) ENCODE ZSTD,
"restriction_code" VARCHAR(4) ENCODE ZSTD,
"status_code" SMALLINT ENCODE ZSTD,
"inventory_code" SMALLINT ENCODE ZSTD,
"date_added" DATE ENCODE ZSTD,
"date_removed" DATE ENCODE ZSTD,
"data_status" VARCHAR(4) ENCODE ZSTD
);

GRANT SELECT ON microservice.ldb_sku TO looker;
GRANT SELECT ON microservice.ldb_sku TO datamodeling;
