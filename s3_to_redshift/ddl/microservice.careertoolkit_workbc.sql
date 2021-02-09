CREATE TABLE IF NOT EXISTS microservice.careertoolkit_workbc
( 
  "grouping" VARCHAR(255) ENCODE ZSTD NOT NULL,
  "noc" VARCHAR(255) ENCODE ZSTD NOT NULL,
  "description" VARCHAR(255) ENCODE ZSTD NOT NULL
);

GRANT SELECT ON microservice.careertoolkit_workbc TO "looker";
