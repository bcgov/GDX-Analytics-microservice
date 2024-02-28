-- taken from the explore: https://analytics.gov.bc.ca/explore/snowplow_web_block/page_views?qid=QY20X97Kqhk3mP1PtMpm24&origin_space=37&toggle=fil,vis
SELECT * FROM (
    SELECT
    "$f1" AS "page_views.page_urlhost",
    (DATE("$f2")) AS "page_views.page_view_start_date",
    "$f0" AS "page_views.referrer_channel",
    COALESCE(SUM("page_views.row_count"), 0) AS "page_views.row_count"
    FROM (
    SELECT
        CASE
        WHEN page_views.refr_medium IS NULL THEN ''direct''
        WHEN page_views.refr_medium = ''unknown'' THEN ''other''
        ELSE page_views.refr_medium 
        END AS "$f0",
        page_views.page_urlhost  AS "$f1",
        DATE_TRUNC(''DAY'',CAST(page_views.page_view_start_time  AS TIMESTAMP(0))) AS "$f2",
        page_views.node_id AS "$f3",
        page_views.page_exclusion_filter AS "$f4",
        page_views.app_id  AS "$f5",
        page_views.page_section AS "$f6",
        page_views.page_subsection  AS "$f7",
        COALESCE(cmslite_themes.theme_id,'''')  AS "$f8",
        COUNT(*) AS "page_views.row_count"
    FROM 
        derived.page_views AS page_views
        LEFT JOIN cmslite.themes  AS cmslite_themes ON page_views.node_id = cmslite_themes.node_id
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
    ) AS "t6"
    WHERE 
    "$f1" = ''www.healthgateway.gov.bc.ca'' 
    AND ((
        ( "$f2" ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) 
        AND ( "$f2" ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) )))
    )) 
    AND ("$f3" IS NOT NULL AND "$f1" IS NOT NULL) 
    AND (
        "$f4" IS NOT NULL 
        AND "$f5" IS NOT NULL 
        AND (
        "$f6" IS NOT NULL 
        AND (
            "$f7" IS NOT NULL 
            AND "$f8" IS NOT NULL
        )
        )
    )
    GROUP BY
        1,
        2,
        3
    ORDER BY
        4 desc
)