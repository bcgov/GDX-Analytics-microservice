SELECT * FROM (
    WITH "new_rows" AS (
        -- based off of the explore https://analytics.gov.bc.ca/explore/cfms_poc/cfms_poc?toggle=fil&qid=VmltsfZUBNw8YZPXmZmNrU
        SELECT
            0 AS "update_flag",
            (DATE("welcome_time")) AS "cfms_poc.date",
                (TO_CHAR(DATE_TRUNC(''second'', "welcome_time"), ''YYYY-MM-DD HH24:MI:SS'')) AS "cfms_poc.welcome_time",
                (TO_CHAR(DATE_TRUNC(''second'', "latest_time"), ''YYYY-MM-DD HH24:MI:SS'')) AS "cfms_poc.latest_time",
            "office_id" AS "cfms_poc.office_id",
            "office_name" AS "cfms_poc.office_name",
            "agent_id" AS "cfms_poc.agent_id",
            "client_id" AS "cfms_poc.client_id",
            "back_office" AS "cfms_poc.back_office",
            "channel_sort" AS "cfms_poc.channel_sort",
            "channel" AS "cfms_poc.channel",
            "counter_type" AS "cfms_poc.counter_type",
            "program_name" AS "cfms_poc.program_name",
            "transaction_name" AS "cfms_poc.transaction_name",
            "service_count" AS "cfms_poc.service_count",
            COALESCE(cfms_poc.status, ''Open Ticket'') AS "cfms_poc.status",
                (CASE WHEN abs(cfms_poc.hold_duration_zscore) >= 3  THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.hold_duration_outlier",
                (CASE WHEN abs(cfms_poc.prep_duration_zscore) >= 3  THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.prep_duration_outlier",
                (CASE WHEN abs( cfms_poc.serve_duration_zscore) >= 3 THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.serve_duration_outlier",
                (CASE WHEN abs(cfms_poc.service_creation_duration_zscore) >= 3  THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.service_creation_duration_outlier",
                (CASE WHEN abs(cfms_poc.waiting_duration_zscore) >= 3  THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.waiting_duration_outlier",
            COUNT(*) AS "cfms_poc.services_count",
            COALESCE(SUM(cfms_poc.transaction_count ), 0) AS "cfms_poc.transactions_count",
            AVG((1.00 * cfms_poc.service_creation_duration)/(60*60*24) ) AS "cfms_poc.service_creation_duration_per_visit_average",
            AVG((1.00 * cfms_poc.waiting_duration)/(60*60*24) ) AS "cfms_poc.waiting_duration_per_service_average",
            AVG((1.00 * cfms_poc.prep_duration)/(60*60*24) ) AS "cfms_poc.prep_duration_per_service_average",
            AVG((1.00 * cfms_poc.serve_duration)/(60*60*24) ) AS "cfms_poc.serve_duration_per_service_average",
            AVG((1.00 * cfms_poc.hold_duration)/(60*60*24) ) AS "cfms_poc.hold_duration_per_service_average"
        FROM
            "derived"."theq_step1" AS "cfms_poc"
        WHERE ((( "welcome_time" ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( "welcome_time" ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) ))))) AND "program_name" IN (''SDPR'', ''SDPR - POC'') AND (TRANSLATE(TRANSLATE(cfms_poc.office_name, '' '', ''_''),''.'','''') ) IS NOT NULL
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
            20,
            21
    ),
    "update_rows" AS (  
        -- based off of the explore https://analytics.gov.bc.ca/explore/cfms_poc/cfms_poc?toggle=fil&qid=Gv2rbpnr3aWhWjYmBRNDUa
        SELECT
            1 AS "update_flag",
            (DATE("welcome_time")) AS "cfms_poc.date",
                (TO_CHAR(DATE_TRUNC(''second'', "welcome_time"), ''YYYY-MM-DD HH24:MI:SS'')) AS "cfms_poc.welcome_time",
                (TO_CHAR(DATE_TRUNC(''second'', "latest_time"), ''YYYY-MM-DD HH24:MI:SS'')) AS "cfms_poc.latest_time",
            "office_id" AS "cfms_poc.office_id",
            "office_name" AS "cfms_poc.office_name",
            "agent_id" AS "cfms_poc.agent_id",
            "client_id" AS "cfms_poc.client_id",
            "back_office" AS "cfms_poc.back_office",
            "channel_sort" AS "cfms_poc.channel_sort",
            "channel" AS "cfms_poc.channel",
            "counter_type" AS "cfms_poc.counter_type",
            "program_name" AS "cfms_poc.program_name",
            "transaction_name" AS "cfms_poc.transaction_name",
            "service_count" AS "cfms_poc.service_count",
            COALESCE(cfms_poc.status, ''Open Ticket'') AS "cfms_poc.status",
                (CASE WHEN abs(cfms_poc.hold_duration_zscore) >= 3  THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.hold_duration_outlier",
                (CASE WHEN abs(cfms_poc.prep_duration_zscore) >= 3  THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.prep_duration_outlier",
                (CASE WHEN abs( cfms_poc.serve_duration_zscore) >= 3 THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.serve_duration_outlier",
                (CASE WHEN abs(cfms_poc.service_creation_duration_zscore) >= 3  THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.service_creation_duration_outlier",
                (CASE WHEN abs(cfms_poc.waiting_duration_zscore) >= 3  THEN ''Yes'' ELSE ''No'' END) AS "cfms_poc.waiting_duration_outlier",
            COUNT(*) AS "cfms_poc.services_count",
            COALESCE(SUM(cfms_poc.transaction_count ), 0) AS "cfms_poc.transactions_count",
            AVG((1.00 * cfms_poc.service_creation_duration)/(60*60*24) ) AS "cfms_poc.service_creation_duration_per_visit_average",
            AVG((1.00 * cfms_poc.waiting_duration)/(60*60*24) ) AS "cfms_poc.waiting_duration_per_service_average",
            AVG((1.00 * cfms_poc.prep_duration)/(60*60*24) ) AS "cfms_poc.prep_duration_per_service_average",
            AVG((1.00 * cfms_poc.serve_duration)/(60*60*24) ) AS "cfms_poc.serve_duration_per_service_average",
            AVG((1.00 * cfms_poc.hold_duration)/(60*60*24) ) AS "cfms_poc.hold_duration_per_service_average"
        FROM
            "derived"."theq_step1" AS "cfms_poc"
        WHERE ((( "latest_time" ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( "latest_time" ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) ))))) AND ((( "welcome_time" ) >= ((TIMESTAMP ''2022-04-01'')) AND ( "welcome_time" ) < ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))))) AND "program_name" IN (''SDPR'', ''SDPR - POC'') AND (TRANSLATE(TRANSLATE(cfms_poc.office_name, '' '', ''_''),''.'','''') ) IS NOT NULL
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
            20,
            21
    )
    SELECT * FROM new_rows UNION SELECT * FROM update_rows
    ORDER BY
        3
)