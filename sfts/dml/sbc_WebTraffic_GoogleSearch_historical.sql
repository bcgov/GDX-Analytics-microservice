SELECT * FROM (
    -- based on the explore https://analytics.gov.bc.ca/explore/google_api/google_search?qid=sbu6iPQADchDYaYXFlobLA&origin_space=37&toggle=fil
    SELECT
        (DATE("date")) AS "google_search.google_search_date",
        "query" AS "google_search.query",
        COALESCE(SUM(google_search.clicks), 0) AS "google_search.total_clicks",
        COALESCE(SUM(google_search.impressions), 0) AS "google_search.total_impressions"
    FROM
        "cmslite"."google_pdt" AS "google_search"
    WHERE ("date") < ((DATEADD(day,-2, DATE_TRUNC(''day'',GETDATE()) ))) AND ((COALESCE(google_search.topic, ''(no topic)'') ) = ''Service BC'' AND "node_id" IS NOT NULL) AND ("page_urlhost" IS NOT NULL AND (COALESCE(google_search.theme_id,'''') ) IS NOT NULL AND ((COALESCE(google_search.subtheme_id,'''')) IS NOT NULL AND (COALESCE(google_search.topic_id,'''') ) IS NOT NULL))
    GROUP BY
        1,
        2
    ORDER BY
        1,
        3 DESC,
        4 DESC
)
