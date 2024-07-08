-- taken from the explore: https://analytics.gov.bc.ca/explore/snowplow_web_block/healthgateway_actions?qid=votmywAPP63WqWZPX9o4PC&origin_space=37&toggle=fil,vis
SELECT * FROM (
    SELECT
        (DATE(healthgateway_actions.timestamp )) AS "healthgateway_actions.event_date",
        healthgateway_actions.text AS "healthgateway_actions.text",
        COUNT(*) AS "healthgateway_actions.count",
        COUNT(DISTINCT healthgateway_actions.session_id  ) AS "healthgateway_actions.session_count"
    FROM looker.NZ_snowplow_web_block_healthgateway_actions AS healthgateway_actions
    WHERE (healthgateway_actions.action) = ''download_report'' AND ((( healthgateway_actions.timestamp  ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( healthgateway_actions.timestamp  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) ))))) AND (healthgateway_actions.page_urlhost ) IS NOT NULL
    GROUP BY
        1,
        2
    ORDER BY
        3 DESC
)
