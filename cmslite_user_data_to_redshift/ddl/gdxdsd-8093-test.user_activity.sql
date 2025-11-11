CREATE TABLE IF NOT EXISTS test.gdxdsd_8093_user_activity
( 
  "user_id" VARCHAR(100) ENCODE ZSTD NOT NULL,
  "user_idir" VARCHAR(100) ENCODE ZSTD,
  "activity_type" VARCHAR(255) ENCODE ZSTD,
  "memo" VARCHAR(255) ENCODE ZSTD,
  "activity_date" TIMESTAMP ENCODE ZSTD,
  "group_name"VARCHAR(255) ENCODE ZSTD
);

GRANT SELECT ON test.gdxdsd_8093_user_activity TO "looker";