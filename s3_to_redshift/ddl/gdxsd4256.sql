CREATE TABLE IF NOT EXISTS test.gdxdsd4256
(
  "Site" INTEGER ENCODE ZSTD NOT NULL,
  "Asset Tag" VARCHAR(20)  ENCODE ZSTD,
  "Printer Queue" VARCHAR(100) ENCODE ZSTD,
  "IDIR ID" INTEGER ENCODE ZSTD,
  "Notes" VARCHAR(512) ENCODE ZSTD,
  "Item Type" VARCHAR(20) ENCODE ZSTD,
  "Path" VARCHAR(32) ENCODE ZSTD
);

GRANT SELECT ON test.gdxdsd4256 TO "looker";
GRANT SELECT ON test.gdxdsd4256 TO "datamodeling";
GRANT SELECT ON test.gdxdsd4256 TO "microservice";