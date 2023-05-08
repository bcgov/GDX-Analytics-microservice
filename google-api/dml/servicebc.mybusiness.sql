BEGIN;
SET SEARCH_PATH TO {schema};
DROP TABLE IF EXISTS {dbtable};
CREATE TABLE {dbtable} AS
SELECT
  gl.*,
  oi.site AS office_site,
  oi.officesize AS office_size,
  oi.area AS area_number,
  oi.id AS office_id,
  oi.current_area as current_area,
  dd.isweekend::BOOLEAN,
  dd.isholiday::BOOLEAN,
  dd.lastdayofpsapayperiod::date,
  dd.fiscalyear,
  dd.fiscalmonth,
  dd.fiscalquarter,
  dd.sbcquarter,
  dd.day,
  dd.weekday,
  dd.weekdayname
FROM google.locations AS gl
JOIN servicebc.datedimension AS dd
ON gl.date::date = dd.datekey::date
LEFT JOIN servicebc.office_info AS oi
ON gl.location_id = oi.google_location_id AND end_date IS NULL;
ALTER TABLE {dbtable} OWNER TO microservice;
GRANT SELECT ON {dbtable} TO looker;
COMMIT;
