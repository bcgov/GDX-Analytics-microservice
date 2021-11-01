CREATE TABLE IF NOT EXISTS microservice.asset_downloads_derived
( 
  "asset_url" VARCHAR(4200) ENCODE ZSTD,
  "date_timestamp" timestamp without time zone ENCODE ZSTD,
  "ip_address" VARCHAR(255) ENCODE ZSTD NOT NULL,
  "request_response_time" VARCHAR(255) ENCODE ZSTD,
  "referrer" VARCHAR(4095) ENCODE ZSTD,
  "return_size" DOUBLE PRECISION ENCODE ZSTD,
  "status_code" INTEGER ENCODE ZSTD,
  "asset_file" VARCHAR(4200) ENCODE ZSTD,
  "asset_ext" VARCHAR(4200) ENCODE ZSTD,
  "user_agent_http_request_header" VARCHAR(4095) ENCODE ZSTD,
  "request_string" VARCHAR(4095) ENCODE ZSTD,
  "asset_host" VARCHAR(4117) ENCODE ZSTD NOT NULL,
  "asset_source" VARCHAR(4117) ENCODE ZSTD NOT NULL,
  "direct_download" BOOL ENCODE ZSTD,
  "offsite_download" BOOL ENCODE ZSTD,
  "is_efficiencybc_dev" BOOL ENCODE ZSTD,
  "is_government" BOOL ENCODE ZSTD,
  "is_mobile" BOOL ENCODE ZSTD,
  "device" VARCHAR(255) ENCODE ZSTD,
  "os_family" VARCHAR(255) ENCODE ZSTD,
  "os_version" VARCHAR(255) ENCODE ZSTD,
  "browser_family" VARCHAR(255) ENCODE ZSTD,
  "browser_version" VARCHAR(255) ENCODE ZSTD,
  "referrer_urlhost_derived" VARCHAR(4095) ENCODE ZSTD,
  "referrer_medium" VARCHAR(255) ENCODE ZSTD,
  "referrer_urlpath" VARCHAR(4095) ENCODE ZSTD,
  "referrer_urlquery" VARCHAR(4095) ENCODE ZSTD,
  "referrer_urlscheme" VARCHAR(4095) ENCODE ZSTD,
  "page_referrer_display_url" VARCHAR(4095) ENCODE ZSTD,
  "asset_url_case_insensitive" VARCHAR(4095) ENCODE ZSTD,
  "asset_url_nopar" VARCHAR(4095) ENCODE ZSTD,
  "asset_url_nopar_case_insensitive" VARCHAR(4095) ENCODE ZSTD,
  "truncated_asset_url_nopar_case_insensitive" VARCHAR(4095) ENCODE ZSTD,
  "sitekey" VARCHAR(20) ENCODE ZSTD
);

GRANT SELECT ON microservice.asset_downloads_derived TO "looker";
