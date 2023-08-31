SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/dpvp2sqgp7pwzm
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
    LEFT JOIN derived.page_views ON ''https://www2.gov.bc.ca''||SPLIT_PART(page_views.page_urlpath,''?'',1) = hr_url
      AND page_views.page_urlhost IN (''www2.gov.bc.ca'', ''alpha.gov.bc.ca'')
      AND (((( page_views.page_view_start_time  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( page_views.page_view_start_time  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) )))))) 
  WHERE (
    hr_url LIKE ''%content/transportation%'' OR 
    hr_url LIKE ''%transportation-projects%'' OR
    hr_url LIKE ''%celebrating-excellence-transportation%'' OR
    hr_url LIKE ''%gov/content/kicking-horse-canyon-project%'' OR
    hr_url LIKE ''%content/industry/construction-industry/transportation-infrastructure%'' OR
    hr_url LIKE ''%land-use-regulation/subdividing-land%'' OR
    hr_url LIKE ''%ministries/transportation-and-infrastructure%'' OR
    hr_url LIKE ''%citizens-services/servicebc%''
  )
  AND site_id = ''CA4CBBBB070F043ACF7FB35FE3FD1081''
  GROUP BY 1,2,3,4,5,6,7,8
  ORDER BY 1,2,4 ASC
)
