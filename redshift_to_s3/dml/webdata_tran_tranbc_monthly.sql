SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/tg782svxjdbzsf
  SELECT 
    page_display_url AS url,
    NULL AS "metadata.node_id",
    NULL AS "metadata.title",
    (DATE(page_views.page_view_start_time )) AS "date",
    NULL AS "metadata.created_date",
    NULL AS "metadata.modified_date",
    NULL AS "metadata.published_date",
    NULL AS "metadata.page_status",
    COUNT(DISTINCT page_views.page_view_id ) AS "page_view_count"
  FROM 
    derived.page_views 
  WHERE page_urlhost = ''www.tranbc.ca''
    AND (((( page_views.page_view_start_time  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( page_views.page_view_start_time  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) )))))) 
  GROUP BY 1,2,3,4,5,6,7,8
  ORDER BY 1,2,4 ASC
)
