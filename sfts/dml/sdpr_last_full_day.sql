SELECT * FROM (
    SELECT
        (DATE("welcome_time")) AS "theq_sdpr_poc.date",
            (TO_CHAR(DATE_TRUNC(''second'', "welcome_time"), ''YYYY-MM-DD HH24:MI:SS'')) AS "theq_sdpr_poc.welcome_time",
            (TO_CHAR(DATE_TRUNC(''second'', "latest_time"), ''YYYY-MM-DD HH24:MI:SS'')) AS "theq_sdpr_poc.latest_time",
        "office_id" AS "theq_sdpr_poc.office_id",
        "office_name" AS "theq_sdpr_poc.office_name",
        "agent_id" AS "theq_sdpr_poc.agent_id",
        "client_id" AS "theq_sdpr_poc.client_id",
        "back_office" AS "theq_sdpr_poc.back_office",
        "channel_sort" AS "theq_sdpr_poc.channel_sort",
        "channel" AS "theq_sdpr_poc.channel",
        "counter_type" AS "theq_sdpr_poc.counter_type",
            (CASE WHEN "inaccurate_time" THEN ''Yes'' ELSE ''No'' END) AS "theq_sdpr_poc.inaccurate_time",
            (CASE WHEN "no_wait_visit" THEN ''Yes'' ELSE ''No'' END) AS "theq_sdpr_poc.no_wait_visit",
        "program_name" AS "theq_sdpr_poc.program_name",
        "transaction_name" AS "theq_sdpr_poc.transaction_name",
        "service_count" AS "theq_sdpr_poc.service_count",
        COALESCE(theq_sdpr_poc.status, ''Open Ticket'') AS "theq_sdpr_poc.status",
        COUNT(DISTINCT theq_sdpr_poc.client_id ) AS "theq_sdpr_poc.visits_count",
        COUNT(*) AS "theq_sdpr_poc.services_count",
        COALESCE(SUM(theq_sdpr_poc.transaction_count ), 0) AS "theq_sdpr_poc.transactions_count",
        AVG((1.00 * theq_sdpr_poc.service_creation_duration)/(60*60*24) ) AS "theq_sdpr_poc.service_creation_duration_per_visit_average",
        AVG((1.00 * theq_sdpr_poc.waiting_duration)/(60*60*24) ) AS "theq_sdpr_poc.waiting_duration_per_service_average",
        AVG((1.00 * theq_sdpr_poc.prep_duration)/(60*60*24) ) AS "theq_sdpr_poc.prep_duration_per_service_average",
        AVG((1.00 * theq_sdpr_poc.serve_duration)/(60*60*24) ) AS "theq_sdpr_poc.serve_duration_per_service_average",
        AVG((1.00 * theq_sdpr_poc.hold_duration)/(60*60*24) ) AS "theq_sdpr_poc.hold_duration_per_service_average"
    FROM
        "derived"."theq_sdpr_step1" AS "theq_sdpr_poc"
    WHERE ((( "welcome_time" ) >= ((DATE_TRUNC(''day'',GETDATE()))) AND ( "welcome_time" ) < ((DATEADD(day,1, DATE_TRUNC(''day'',GETDATE()) ))))) AND (TRANSLATE(TRANSLATE(theq_sdpr_poc.office_name, '' '', ''_''),''.'','''') ) IS NOT NULL
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
        17
    ORDER BY
        2
)
