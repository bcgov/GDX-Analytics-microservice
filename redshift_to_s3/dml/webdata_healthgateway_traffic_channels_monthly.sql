-- taken from the explore: https://analytics.gov.bc.ca/explore/snowplow_web_block/page_views?qid=KlAKYVPg8pceUuTeuC26oU&origin_space=37&toggle=fil,vis
SELECT * FROM (
    SELECT
    "$f1" AS "page_views.page_urlhost",
    (TO_CHAR(DATE_TRUNC(''month'', "$f2"), ''YYYY-MM'')) AS "page_views.page_view_start_month",
    "$f0" AS "page_views.referrer_channel",
    COALESCE(SUM("page_views.row_count"), 0) AS "page_views.row_count"
    FROM (
    SELECT
        CASE
        WHEN page_views.refr_medium IS NULL THEN ''direct''
        WHEN page_views.refr_medium = ''unknown'' THEN ''other''
        ELSE  page_views.refr_medium 
        END AS "$f0",
        page_views.page_urlhost  AS "$f1",
        DATE_TRUNC(''MONTH'',CAST(page_views.page_view_start_time  AS TIMESTAMP(0))) AS "$f2",
        DATE_TRUNC(''DAY'',CAST(page_views.page_view_start_time  AS TIMESTAMP(0))) AS "$f3",
        page_views.node_id AS "$f4",
        page_views.page_exclusion_filter AS "$f5",
        page_views.app_id  AS "$f6",
        page_views.page_section AS "$f7",
        page_views.page_subsection  AS "$f8",
        COALESCE(cmslite_themes.theme_id,'''')  AS "$f9",
        COUNT(*) AS "page_views.row_count"
    FROM 
        derived.page_views  AS page_views
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
        9,
        10
    ) AS "t7"
    WHERE 
    "$f1" = ''www.healthgateway.gov.bc.ca'' 
    AND ((
        ( "$f3" ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) 
        AND ( "$f3" ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) )))
    )) 
    AND (
        "$f4" IS NOT NULL 
        AND "$f1" IS NOT NULL
    ) 
    AND (
        "$f5" IS NOT NULL 
        AND "$f6" IS NOT NULL 
        AND (
        "$f7" IS NOT NULL 
        AND (
            "$f8" IS NOT NULL 
            AND "$f9" IS NOT NULL
        )
        )
    )
    GROUP BY
        1,
        2,
        3
    ORDER BY
        "$f0"
)