-- taken from the explore: https://analytics.gov.bc.ca/explore/snowplow_web_block/healthgateway_actions?qid=zqIXt6d2nkeO9JG4h4PRdC&origin_space=37&toggle=fil,vis
SELECT * FROM (
    SELECT
        (DATE(healthgateway_actions.timestamp )) AS "healthgateway_actions.event_date",
        healthgateway_actions.text AS "healthgateway_actions.text",
        COUNT(*) AS "healthgateway_actions.count"
    FROM looker.NZ_snowplow_web_block_healthgateway_actions AS healthgateway_actions
    WHERE (healthgateway_actions.action) = ''submit_app_rating'' AND ((( healthgateway_actions.timestamp  ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( healthgateway_actions.timestamp  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) ))))) AND (healthgateway_actions.page_urlhost ) IS NOT NULL
    GROUP BY
        1,
        2
    ORDER BY
        2
)
