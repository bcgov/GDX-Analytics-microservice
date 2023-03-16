SELECT * FROM (
  -- based off of the explore https://analytics.gov.bc.ca/explore/snowplow_web_block/sbc_online_appointments_clicks?qid=4Aj5Ua6FHKbjY7ognYW2aP&origin_space=37&toggle=fil,vis
  -- using the stable name PDT BM_snowplow_web_block_sbc_online_appointments_clicks
    SELECT
        (TO_CHAR(DATE_TRUNC(''second'', sbc_online_appointments_clicks.timestamp ), ''YYYY-MM-DD HH24:MI:SS'')) AS "sbc_online_appointments_clicks.click_time",
        sbc_online_appointments_clicks.session_id AS "sbc_online_appointments_clicks.session_id",
        sbc_online_appointments_clicks.service AS "sbc_online_appointments_clicks.service",
        sbc_online_appointments_clicks.url AS "sbc_online_appointments_clicks.url",
        sbc_online_appointments_clicks.appointment_step AS "sbc_online_appointments_clicks.appointment_step",
        sbc_online_appointments_clicks.step_order AS "sbc_online_appointments_clicks.step_order",
        COUNT(DISTINCT sbc_online_appointments_clicks.session_id) AS "sbc_online_appointments_clicks.session_count"
    FROM looker.BM_snowplow_web_block_sbc_online_appointments_clicks AS sbc_online_appointments_clicks
    LEFT JOIN derived.page_views  AS page_views ON page_views.page_view_id = sbc_online_appointments_clicks.page_view_id
    WHERE ((( sbc_online_appointments_clicks.timestamp  ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( sbc_online_appointments_clicks.timestamp  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) ))))) AND (sbc_online_appointments_clicks.label) = ''Online Option'' AND (page_views.page_urlhost ) IS NOT NULL
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6
    ORDER BY
        1 DESC
)
