SELECT * FROM (
    -- based on the explore https://analytics.gov.bc.ca/explore/snowplow_web_block/page_views?qid=PUtKfBOFzEGGrMtPNdF9pH&origin_space=37&toggle=fil,pik
    SELECT
        sessions.session_id  AS "sessions.session_id",
        sessions.first_page_url  AS "sessions.first_page_url",
            (TO_CHAR(DATE_TRUNC(''second'', sessions.session_start ), ''YYYY-MM-DD HH24:MI:SS'')) AS "sessions.session_start_time",
            (TO_CHAR(DATE_TRUNC(''second'', sessions.session_end ), ''YYYY-MM-DD HH24:MI:SS'')) AS "sessions.session_end_time",
        DATEDIFF(SECONDS, sessions.session_start, sessions.session_end)  AS "sessions.session_length",
        CASE WHEN sessions.geo_latitude  IS NOT NULL AND sessions.geo_longitude  IS NOT NULL THEN (
    COALESCE(CAST(sessions.geo_latitude  AS VARCHAR),'''') || '','' ||
    COALESCE(CAST(sessions.geo_longitude  AS VARCHAR),'''')) ELSE NULL END
     AS "sessions.geo_location",
        sessions.geo_city  AS "sessions.geo_city",
        sessions.ip_isp  AS "sessions.ip_isp",
        sessions.dvce_type  AS "sessions.device_type",
        sessions.br_family  AS "sessions.browser_family",
            (CASE WHEN sessions.is_government THEN ''Yes'' ELSE ''No'' END) AS "sessions.is_government",
            (CASE WHEN sessions.dvce_ismobile  THEN ''Yes'' ELSE ''No'' END) AS "sessions.device_is_mobile",
        CASE
              WHEN sessions.refr_medium IS NULL THEN ''direct''
              WHEN sessions.refr_medium = ''unknown'' THEN ''other''
              ELSE  sessions.refr_medium END AS "sessions.referrer_medium"
    FROM derived.page_views  AS page_views
    LEFT JOIN derived.sessions  AS sessions ON sessions.session_id = page_views.session_id
    LEFT JOIN cmslite.themes  AS cmslite_themes ON page_views.node_id = cmslite_themes.node_id
    WHERE (sessions.session_start ) < ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND (COALESCE(cmslite_themes.topic, ''(no topic)'') ) = ''Service BC'' AND ((page_views.node_id) IS NOT NULL AND (page_views.page_urlhost ) IS NOT NULL) AND ((page_views.page_exclusion_filter) IS NOT NULL AND (page_views.app_id ) IS NOT NULL AND ((page_views.page_section) IS NOT NULL AND ((page_views.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'''') ) IS NOT NULL)))
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
        12,
        13
    ORDER BY
        3
)
