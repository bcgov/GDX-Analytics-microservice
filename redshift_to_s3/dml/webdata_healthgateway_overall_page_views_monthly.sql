-- taken from the explore: https://analytics.gov.bc.ca/explore/snowplow_web_block/page_views?toggle=fil&qid=0Ncbpo1nzaCFIrhVYgtewj
SELECT * FROM (
    SELECT
        (TO_CHAR(DATE_TRUNC(''month'', page_views.page_view_start_time ), ''YYYY-MM'')) AS "page_views.page_view_start_month",
        page_views.page_urlhost  AS "page_views.page_urlhost",
        COUNT(DISTINCT page_views.page_view_id ) AS "page_views.page_view_count",
        COUNT(DISTINCT page_views.session_id ) AS "page_views.session_count",
        COUNT(DISTINCT page_views.domain_userid ) AS "page_views.user_count"
    FROM derived.page_views  AS page_views
    LEFT JOIN cmslite.themes  AS cmslite_themes ON page_views.node_id = cmslite_themes.node_id
    WHERE (page_views.page_urlhost ) LIKE ''%www.healthgateway.gov.bc.ca%'' AND ((( page_views.page_view_start_time  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( page_views.page_view_start_time  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) ))))) AND ((page_views.node_id) IS NOT NULL AND (page_views.page_urlhost ) IS NOT NULL) AND ((page_views.page_exclusion_filter) IS NOT NULL AND (page_views.app_id ) IS NOT NULL AND ((page_views.page_section) IS NOT NULL AND ((page_views.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'''') ) IS NOT NULL)))
    GROUP BY
        (DATE_TRUNC(''month'', page_views.page_view_start_time )),
        2
    ORDER BY
        1 DESC
)