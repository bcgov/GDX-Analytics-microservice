CREATE TABLE IF NOT EXISTS test.LDB_Data
( 
  "sku" BIGINT ENCODE ZSTD NOT NULL,
  "product_name" VARCHAR ENCODE ZSTD,
  "image" VARCHAR ENCODE ZSTD,
  "body" VARCHAR ENCODE ZSTD,
  "volume" VARCHAR ENCODE ZSTD,
  "bottles_per_pack" VARCHAR ENCODE ZSTD,
  "regular_price" VARCHAR ENCODE ZSTD,
  "lto_price" VARCHAR ENCODE ZSTD,
  "lto_start" VARCHAR ENCODE ZSTD,
  "lto_end" VARCHAR ENCODE ZSTD,
  "price_override" VARCHAR ENCODE ZSTD,
  "store_count" VARCHAR ENCODE ZSTD,
  "inventory" VARCHAR ENCODE ZSTD,
  "availability_override" VARCHAR ENCODE ZSTD,
  "whitelist" VARCHAR ENCODE ZSTD,
  "blacklist" VARCHAR ENCODE ZSTD,
  "upc" VARCHAR ENCODE ZSTD,
  "all_upcs" VARCHAR ENCODE ZSTD,
  "alcohol" VARCHAR ENCODE ZSTD,
  "kosher" VARCHAR ENCODE ZSTD,
  "organic" VARCHAR ENCODE ZSTD,
  "sweetness" VARCHAR ENCODE ZSTD,
  "vqa" VARCHAR ENCODE ZSTD,
  "craft_beer" VARCHAR ENCODE ZSTD,
  "bcl_select" VARCHAR ENCODE ZSTD,
  "new" VARCHAR ENCODE ZSTD,
  "rating" VARCHAR ENCODE ZSTD,
  "votes" VARCHAR ENCODE ZSTD,
  "product_type" VARCHAR ENCODE ZSTD,
  "category" VARCHAR ENCODE ZSTD,
  "sub_category" VARCHAR ENCODE ZSTD,
  "country" VARCHAR ENCODE ZSTD,
  "region" VARCHAR ENCODE ZSTD,
  "sub_region" VARCHAR ENCODE ZSTD,
  "grape_variety" VARCHAR ENCODE ZSTD,
  "restriction_code" VARCHAR ENCODE ZSTD,
  "status_code" VARCHAR ENCODE ZSTD,
  "inventory_code" VARCHAR ENCODE ZSTD
);
GRANT SELECT ON test.LDB_Data TO "looker";
GRANT SELECT ON test.LDB_Data TO "datamodeling";