SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/czsxmfzxnhmtdg
  SELECT 
    metadata.hr_url,
    metadata.node_id,
    metadata.title,
    (DATE(page_views.page_view_start_time )) AS "date",
    metadata.created_date,
    metadata.modified_date,
    metadata.published_date,
    metadata.page_status,
    COUNT(DISTINCT page_views.page_view_id ) AS "page_view_count"
  FROM 
    cmslite.metadata 
    LEFT JOIN derived.page_views ON ''https://intranet.gov.bc.ca''||SPLIT_PART(page_views.page_urlpath,''?'',1) = hr_url
      AND page_views.page_urlhost IN (''intranet.gov.bc.ca'')
      AND (((( page_views.page_view_start_time  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( page_views.page_view_start_time  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) )))))) 
  WHERE (
    hr_url LIKE ''https://intranet.gov.bc.ca/trannet%'' 
    AND site_id = ''A9A4B738CE26466C92B45A66DD8C2AFC''
  )
  GROUP BY 1,2,3,4,5,6,7,8
  ORDER BY 1,2,4 ASC
)
