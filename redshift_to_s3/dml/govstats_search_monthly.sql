SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/2xczvvqwwb7ywt
  SELECT
      (TO_CHAR(DATE_TRUNC(''month'', searches.collector_tstamp ), ''YYYY-MM'')) AS "searches.search_time_month",
      searches.terms  AS "searches.search_terms",
      COUNT(DISTINCT searches.search_id ) AS "searches.search_count"
  FROM derived.searches  AS searches
  LEFT JOIN cmslite.themes  AS cmslite_themes ON searches.node_id = cmslite_themes.node_id
  WHERE (searches.page_urlhost ) = ''www2.gov.bc.ca'' AND (((( searches.collector_tstamp  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( searches.collector_tstamp  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) ))))) AND LENGTH(searches.terms ) <> 0) AND ((COALESCE(cmslite_themes.subtheme_id,'''') ) = ''38F05F29D17349919D47C5FEFE3EC0E3'' AND ((searches.terms ) IS NOT NULL AND (searches.node_id) IS NOT NULL)) AND ((searches.page_urlhost ) IS NOT NULL AND ((searches.page_exclusion_filter) IS NOT NULL AND (searches.app_id ) IS NOT NULL) AND ((searches.page_section) IS NOT NULL AND ((searches.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'''') ) IS NOT NULL)))
  GROUP BY
      (DATE_TRUNC(''month'', searches.collector_tstamp )),
      2
  ORDER BY
      3 DESC
)