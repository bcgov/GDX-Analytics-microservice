-- taken from the explore: https://analytics.gov.bc.ca/explore/snowplow_web_block/healthgateway_actions?qid=wcvxK6B0X25KmqKjxUOeGG&origin_space=37&toggle=fil,vis
SELECT * FROM (
    SELECT
        (TO_CHAR(DATE_TRUNC(''month'', healthgateway_actions.timestamp ), ''YYYY-MM'')) AS "healthgateway_actions.event_month",
        healthgateway_actions.text AS "healthgateway_actions.text",
        COUNT(*) AS "healthgateway_actions.count",
        COUNT(DISTINCT healthgateway_actions.session_id  ) AS "healthgateway_actions.session_count"
    FROM looker.BM_snowplow_web_block_healthgateway_actions AS healthgateway_actions
    WHERE (healthgateway_actions.action) = ''download_report'' AND ((( healthgateway_actions.timestamp  ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( healthgateway_actions.timestamp  ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) ))))) AND (healthgateway_actions.page_urlhost ) IS NOT NULL
    GROUP BY
        (DATE_TRUNC(''month'', healthgateway_actions.timestamp )),
        2
    ORDER BY
        3 DESC
)