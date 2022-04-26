SELECT * FROM (
    SELECT
        page_views.page_view_id  AS "page_views.page_view_id",
        page_views.session_id  AS "page_views.session_id",
        page_views.page_display_url  AS "page_views.page_display_url",
        (TO_CHAR(DATE_TRUNC('second', page_views.page_view_start_time ), 'YYYY-MM-DD HH24:MI:SS')) AS "page_views.page_view_start_time",
        page_views.page_view_in_session_index  AS "page_views.page_view_in_session_index",
        page_views.node_id AS "page_views.node_id",
        COALESCE(cmslite_themes.theme, '(no theme)')  AS "cmslite_themes.theme",
        COALESCE(cmslite_themes.subtheme, '(no subtheme)')  AS "cmslite_themes.subtheme",
        COALESCE(cmslite_themes.topic, '(no topic)')  AS "cmslite_themes.topic",
        COALESCE(cmslite_themes.subtopic, '(no subtopic)')  AS "cmslite_themes.subtopic",
        cmslite_themes.title  AS "cmslite_themes.title",
        page_views.page_referrer_display_url AS "page_views.page_referrer_display_url",
        CASE WHEN page_views.geo_latitude  IS NOT NULL AND page_views.geo_longitude  IS NOT NULL THEN (
    COALESCE(CAST(page_views.geo_latitude  AS VARCHAR),'') || ',' ||
    COALESCE(CAST(page_views.geo_longitude  AS VARCHAR),'')) ELSE NULL END
    AS "page_views.geo_location",
        page_views.geo_city  AS "page_views.geo_city",
        page_views.ip_isp  AS "page_views.ip_isp",
        page_views.dvce_type  AS "page_views.device_type",
        page_views.br_family  AS "page_views.browser_family",
        (CASE WHEN page_views.is_government THEN 'Yes' ELSE 'No' END) AS "page_views.is_government",
        (CASE WHEN page_views.dvce_ismobile  THEN 'Yes' ELSE 'No' END) AS "page_views.device_is_mobile",
        CASE
            WHEN page_views.refr_medium IS NULL THEN 'direct'
            WHEN page_views.refr_medium = 'unknown' THEN 'other'
            ELSE  page_views.refr_medium END AS "page_views.referrer_medium"
    FROM derived.page_views  AS page_views
    LEFT JOIN cmslite.themes  AS cmslite_themes ON page_views.node_id = cmslite_themes.node_id
    WHERE ((( page_views.page_view_start_time  ) >= ((DATEADD(day,-1, DATE_TRUNC('day',GETDATE()) ))) AND ( page_views.page_view_start_time  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC('day',GETDATE()) ) ))))) AND (COALESCE(cmslite_themes.topic, '(no topic)') ) = 'Service BC' AND ((page_views.node_id) IS NOT NULL AND (page_views.page_urlhost ) IS NOT NULL) AND ((page_views.page_exclusion_filter) IS NOT NULL AND (page_views.app_id ) IS NOT NULL AND ((page_views.page_section) IS NOT NULL AND ((page_views.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'') ) IS NOT NULL)))
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
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20
    ORDER BY
        4
)
