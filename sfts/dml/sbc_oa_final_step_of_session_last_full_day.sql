SELECT * FROM (
    -- based off of the explore https://analytics.gov.bc.ca/explore/snowplow_web_block/sbc_online_appointments?qid=XWockxwvaClhUQh0PaPrXH&origin_space=37&toggle=fil,vis
    -- using the stable name PDT BM_snowplow_web_block_sbc_online_appointments
    SELECT
        (TO_CHAR(DATE_TRUNC(''second'', sbc_online_appointments.min_timestamp ), ''YYYY-MM-DD HH24:MI:SS'')) AS "sbc_online_appointments.min_time_time",
            (TO_CHAR(DATE_TRUNC(''second'', sbc_online_appointments.max_timestamp ), ''YYYY-MM-DD HH24:MI:SS'')) AS "sbc_online_appointments.max_time_time",
        sbc_online_appointments.session_id AS "sbc_online_appointments.session_id",
        sbc_online_appointments.appointment_step AS "sbc_online_appointments.appointment_step",
        sbc_online_appointments.client_id AS "sbc_online_appointments.client_id",
        sbc_online_appointments.appointment_id AS "sbc_online_appointments.appointment_id"
    FROM looker.BM_snowplow_web_block_sbc_online_appointments AS sbc_online_appointments
    WHERE ((( sbc_online_appointments.min_timestamp  ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( sbc_online_appointments.min_timestamp  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) )))))
    GROUP BY -- decided to include GROUP BY to clean up duplicate rows, but this is not a necessary step and could be removed
        1,
        2,
        3,
        4,
        5,
        6
    ORDER BY
        1
)