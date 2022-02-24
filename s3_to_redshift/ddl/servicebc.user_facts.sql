CREATE TABLE IF NOT EXISTS servicebc.user_facts 
(
  "id" INT NOT NULL ENCODE ZSTD,
  "user_id" INT ENCODE ZSTD,
  "external_id" VARCHAR(255) ENCODE ZSTD,
  "is_admin" INT ENCODE ZSTD,
  "is_developer" INT ENCODE ZSTD,
  "is_explorer" INT ENCODE ZSTD,
  "is_embed" INT ENCODE ZSTD,
  "has_ui_credentials" INT ENCODE ZSTD,
  "active_ui_sessions" INT ENCODE ZSTD,
  "last_ui_login_at" TIMESTAMP ENCODE ZSTD,
  "last_ui_login_credential_type" VARCHAR(255) ENCODE ZSTD,
  "has_api_credentials" INT ENCODE ZSTD,
  "active_api_sessions" INT ENCODE ZSTD,
  "last_api_login_at" TIMESTAMP ENCODE ZSTD,
  "is_viewer" INT ENCODE ZSTD,
  "is_presumed_looker_employee" INT ENCODE ZSTD,
  "is_verified_looker_employee" INT ENCODE ZSTD,
  "is_content_saver" INT ENCODE ZSTD,
  "is_sql_runner" INT ENCODE ZSTD
);

GRANT SELECT ON servicebc.user_facts TO "looker";