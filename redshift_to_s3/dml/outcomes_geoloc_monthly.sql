SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/mjghm5hpxgff6r
  SELECT
      (DATE(page_views.page_view_start_time )) AS "page_views.page_view_start_date",
      page_views.geo_country_name  AS "page_views.geo_country_name",
      page_views.geo_region_name  AS "page_views.geo_region_name",
      COUNT(DISTINCT page_views.page_view_id ) AS "page_views.page_view_count",
      COUNT(DISTINCT page_views.session_id ) AS "page_views.session_count",
      COUNT(DISTINCT page_views.domain_userid ) AS "page_views.user_count"
  FROM derived.page_views  AS page_views
  LEFT JOIN cmslite.themes  AS cmslite_themes ON page_views.node_id = cmslite_themes.node_id
  WHERE (page_views.page_urlhost ) = ''outcomes.bcstats.gov.bc.ca'' AND ((( page_views.page_view_start_time  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( page_views.page_view_start_time  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) ))))) AND ((page_views.node_id) IS NOT NULL AND (page_views.page_urlhost ) IS NOT NULL) AND ((page_views.page_exclusion_filter) IS NOT NULL AND (page_views.app_id ) IS NOT NULL AND ((page_views.page_section) IS NOT NULL AND ((page_views.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'''') ) IS NOT NULL)))
  GROUP BY
      1,
      2,
      3
  ORDER BY
      1
)