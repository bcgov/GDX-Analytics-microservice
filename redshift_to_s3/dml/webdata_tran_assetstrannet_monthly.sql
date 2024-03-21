SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/jxjzbqcf9npr8x
SELECT 
  metadata.hr_url,
  metadata.node_id,
  metadata.title,
  (DATE(asset_downloads.date_timestamp )) AS "date",
  metadata.created_date,
  metadata.modified_date,
  metadata.published_date,
  metadata.page_status,
  COUNT(asset_downloads.date_timestamp) AS download_count
  --COUNT(DISTINCT page_views.page_view_id ) AS "page_view_count"
FROM 
  cmslite.metadata 
  LEFT JOIN microservice.asset_downloads_derived  AS asset_downloads 
    ON SPLIT_PART(SPLIT_PART(asset_downloads.asset_url_nopar_case_insensitive,''?'', 1), ''#'', 1) = hr_url
    AND (((( asset_downloads.date_timestamp  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) 
    AND ( asset_downloads.date_timestamp  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) )))))) 
WHERE
  hr_url LIKE ''https://intranet.gov.bc.ca/assets/intranet/trannet%''
  AND site_id = ''A9A4B738CE26466C92B45A66DD8C2AFC''
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 1,2,4 ASC
)