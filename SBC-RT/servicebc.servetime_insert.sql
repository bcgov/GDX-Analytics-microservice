BEGIN;
DROP TABLE IF EXISTS servicebc.servetime;
CREATE TABLE IF NOT EXISTS servicebc.servetime (
  office_id BIGINT ENCODE ZSTD,
  office_name VARCHAR(255) ENCODE ZSTD,
  time_per FLOAT ENCODE ZSTD
);
GRANT SELECT ON servicebc.servetime TO looker;

INSERT INTO servicebc.servetime (
WITH raw_list AS (
  SELECT office_id, office_name, COUNT(DISTINCT client_id) AS client_count, SUM(COALESCE(prep_duration,0) + COALESCE(serve_duration,0)) AS agent_time -- DATE_TRUNC('day', welcome_time),
  FROM derived.theq_step1
  WHERE "back_office" = 'Front Office' AND ((( "welcome_time" ) >= ((DATEADD(day,-7, DATE_TRUNC('day',GETDATE()) ))) AND ( "welcome_time" ) < ((DATEADD(day,7, DATEADD(day,-7, DATE_TRUNC('day',GETDATE()) ) ))))) AND LENGTH("office_name") <> 0 AND (( "office_name"  IS NOT NULL)) AND (NOT "inaccurate_time" OR "inaccurate_time" IS NULL)
  GROUP BY 1,2
  ORDER  BY 1
)
SELECT office_id, office_name, (agent_time / client_count) AS time_per
FROM raw_list
ORDER BY office_id ASC
);

COMMIT;
