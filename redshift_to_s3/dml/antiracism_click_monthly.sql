SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/6vmr3jssbmwjxm
  SELECT
      (DATE(clicks.derived_tstamp )) AS "clicks.click_time_date",
      clicks.target_display_url AS "clicks.target_display_url",
      COUNT(DISTINCT clicks.click_id ) AS "clicks.click_count"
  FROM derived.clicks  AS clicks
  LEFT JOIN cmslite.themes  AS cmslite_themes ON clicks.node_id = cmslite_themes.node_id
  WHERE (clicks.page_urlhost ) = ''antiracism.gov.bc.ca'' AND ((( clicks.derived_tstamp  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( clicks.derived_tstamp  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) ))))) AND ((clicks.node_id) IS NOT NULL AND (clicks.page_urlhost ) IS NOT NULL) AND ((clicks.page_exclusion_filter) IS NOT NULL AND (clicks.app_id ) IS NOT NULL AND ((clicks.page_section) IS NOT NULL AND ((clicks.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'''') ) IS NOT NULL)))
  GROUP BY
      1,
      2
  ORDER BY
      1
)