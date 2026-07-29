SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/2xczvvqwwb7ywt
  SELECT
      (TO_CHAR(DATE_TRUNC(''month'', "date"), ''YYYY-MM'')) AS "google_search.google_search_month",
      "query" AS "google_search.query",
      "page" AS "google_search.page",
      COALESCE(SUM(google_search.clicks), 0) AS "google_search.total_clicks",
      COALESCE(SUM(google_search.impressions), 0) AS "google_search.total_impressions"
  FROM
      "cmslite"."google_dt" AS "google_search"
  WHERE ((( "date" ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( "date" ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) ))))) AND (COALESCE(google_search.subtheme_id,'''')) = ''38F05F29D17349919D47C5FEFE3EC0E3'' AND ("node_id" IS NOT NULL AND "page_urlhost" IS NOT NULL) AND ((COALESCE(google_search.theme_id,'''') ) IS NOT NULL AND (COALESCE(google_search.subtheme_id,'''')) IS NOT NULL AND ((COALESCE(google_search.topic_id,'''') ) IS NOT NULL AND (COALESCE(SPLIT_PART(google_search.page,''/'',4),'''') ) IS NOT NULL))
  GROUP BY
      (DATE_TRUNC(''month'', "date")),
      2,
      3
  ORDER BY
      4 DESC
)