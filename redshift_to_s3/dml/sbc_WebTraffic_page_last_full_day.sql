SELECT * FROM (
    -- based off the explore https://analytics.gov.bc.ca/explore/snowplow_web_block/page_views?qid=PKVWbzPzOKlHPSb2QWcJbp&toggle=fil
    SELECT
        page_views.page_view_id  AS "page_views.page_view_id",
        page_views.session_id  AS "page_views.session_id",
        page_views.page_display_url  AS "page_views.page_display_url",
            (TO_CHAR(DATE_TRUNC(''second'', page_views.page_view_start_time ), ''YYYY-MM-DD HH24:MI:SS'')) AS "page_views.page_view_start_time",
        page_views.page_view_in_session_index  AS "page_views.page_view_in_session_index",
        cmslite_themes.node_id  AS "cmslite_themes.node_id",
        COALESCE(cmslite_themes.theme, ''(no theme)'')  AS "cmslite_themes.theme",
        COALESCE(cmslite_themes.subtheme, ''(no subtheme)'')  AS "cmslite_themes.subtheme",
        COALESCE(cmslite_themes.topic, ''(no topic)'')  AS "cmslite_themes.topic",
        COALESCE(cmslite_themes.subtopic, ''(no subtopic)'')  AS "cmslite_themes.subtopic",
        cmslite_themes.title  AS "cmslite_themes.title",
        page_views.page_referrer_display_url AS "page_views.page_referrer_display_url"
    FROM derived.page_views  AS page_views
    LEFT JOIN cmslite.themes  AS cmslite_themes ON page_views.node_id = cmslite_themes.node_id
    WHERE ((( page_views.page_view_start_time  ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( page_views.page_view_start_time  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) ))))) AND (COALESCE(cmslite_themes.topic, ''(no topic)'') ) = ''Service BC'' AND ((page_views.node_id) IS NOT NULL AND (page_views.page_urlhost ) IS NOT NULL) AND ((page_views.page_exclusion_filter) IS NOT NULL AND (page_views.app_id ) IS NOT NULL AND ((page_views.page_section) IS NOT NULL AND ((page_views.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'''') ) IS NOT NULL)))
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
        10,
        11,
        12
    ORDER BY
        4
)
