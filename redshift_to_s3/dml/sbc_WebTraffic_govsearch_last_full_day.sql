SELECT * FROM (
    -- based off the explore https://analytics.gov.bc.ca/explore/snowplow_web_block/searches?qid=S4m6P6HjLd0IHip16aX6lM&origin_space=37&toggle=fil,vis 
    SELECT
        (TO_CHAR(DATE_TRUNC(''second'', searches.collector_tstamp ), ''YYYY-MM-DD HH24:MI:SS'')) AS "searches.search_time_time",
        searches.search_id  AS "searches.search_id",
        searches.session_id  AS "searches.session_id",
        searches.terms  AS "searches.search_terms_gov"
    FROM derived.searches  AS searches
    LEFT JOIN cmslite.themes  AS cmslite_themes ON searches.node_id = cmslite_themes.node_id
    WHERE ((( searches.collector_tstamp  ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( searches.collector_tstamp  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) ))))) AND LENGTH(searches.terms ) <> 0 AND ((COALESCE(cmslite_themes.topic, ''(no topic)'') ) = ''Service BC'' AND ((( searches.node_id ) ILIKE  ''%'') AND (( searches.page_urlhost  ) ILIKE  ''%''))) AND ((( searches.page_exclusion_filter ) ILIKE  ''%'') AND ((( searches.app_id  ) ILIKE  ''%'') AND (( searches.page_section ) ILIKE  ''%'')) AND ((( searches.page_subsection  ) ILIKE  ''%'') AND ((( COALESCE(cmslite_themes.theme_id,'''')  ) ILIKE  ''%'') AND (searches.terms ) IS NOT NULL)))
    GROUP BY
        1,
        2,
        3,
        4
    ORDER BY
        1
)
