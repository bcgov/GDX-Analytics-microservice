SELECT * FROM (
    SELECT
        searches.terms  AS "searches.search_terms_gov",
        searches.session_id  AS "searches.session_id",
            (TO_CHAR(DATE_TRUNC(''second'', searches.collector_tstamp ), ''YYYY-MM-DD HH24:MI:SS'')) AS "searches.search_time_time"
    FROM derived.searches  AS searches
    LEFT JOIN cmslite.themes  AS cmslite_themes ON searches.node_id = cmslite_themes.node_id
    WHERE ((( searches.collector_tstamp  ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( searches.collector_tstamp  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) ))))) AND LENGTH(searches.terms ) <> 0 AND ((COALESCE(cmslite_themes.topic, ''(no topic)'') ) = ''Service BC'' AND ((searches.terms ) IS NOT NULL AND (searches.node_id) IS NOT NULL)) AND ((searches.page_urlhost ) IS NOT NULL AND ((searches.page_exclusion_filter) IS NOT NULL AND (searches.app_id ) IS NOT NULL) AND ((searches.page_section) IS NOT NULL AND ((searches.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'''') ) IS NOT NULL)))
    GROUP BY
        1,
        2,
        3
    ORDER BY
        1
)
