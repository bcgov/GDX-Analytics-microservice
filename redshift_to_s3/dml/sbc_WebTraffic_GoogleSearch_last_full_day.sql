SELECT * FROM (
    -- based on the explore https://analytics.gov.bc.ca/explore/google_api/google_search?qid=bIW3UKJykygiwk515BK5ln&origin_space=37&toggle=fil
    -- modified by explicitly setting the timezone of the current datetime before conversaion to date
    SELECT
        (DATE("date")) AS "google_search.google_search_date",
        "query" AS "google_search.query",
        COALESCE(SUM(google_search.clicks), 0) AS "google_search.total_clicks",
        COALESCE(SUM(google_search.impressions), 0) AS "google_search.total_impressions"
    FROM
        "cmslite"."google_pdt" AS "google_search"
    WHERE ((( "date" ) >= ((DATEADD(day,-3, DATE_TRUNC(''day'',CONVERT_TIMEZONE(''America/\Vancouver'', GETDATE())) ))) AND ( "date" ) < ((DATEADD(day,1, DATEADD(day,-3, DATE_TRUNC(''day'',CONVERT_TIMEZONE(''America/\Vancouver'', GETDATE())) ) ))))) AND ((COALESCE(google_search.topic, ''(no topic)'') ) = ''Service BC'' AND "node_id" IS NOT NULL) AND ("page_urlhost" IS NOT NULL AND (COALESCE(google_search.theme_id,'''') ) IS NOT NULL AND ((COALESCE(google_search.subtheme_id,'''')) IS NOT NULL AND (COALESCE(google_search.topic_id,'''') ) IS NOT NULL))
    GROUP BY
        1,
        2
    ORDER BY
        1,
        3 DESC,
        4 DESC
)