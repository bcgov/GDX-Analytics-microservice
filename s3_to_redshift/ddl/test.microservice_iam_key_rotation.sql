CREATE TABLE IF NOT EXISTS test.microservice_iam_key_rotation
( 
  "date_of_test" VARCHAR(20) ENCODE ZSTD
);

GRANT SELECT ON test.microservice_iam_key_rotation TO "looker";
