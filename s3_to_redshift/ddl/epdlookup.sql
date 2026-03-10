CREATE TABLE IF NOT EXISTS microservice.epdlookup
( 
  "node_id" VARCHAR(32) ENCODE ZSTD NOT NULL,
  "report_branch" VARCHAR(128) ENCODE ZSTD,
  "ministry" VARCHAR(128) ENCODE ZSTD,
  "division" VARCHAR(128) ENCODE ZSTD,
  "branch" VARCHAR(128) ENCODE ZSTD,
  "program_area" VARCHAR(128) ENCODE ZSTD,
  "program_area_from_smes" VARCHAR(128) ENCODE ZSTD
);

GRANT SELECT ON microservice.epdlookup TO "looker";
