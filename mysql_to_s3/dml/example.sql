-- taken from the explore: https://analytics.gov.bc.ca/explore/system__activity/history?qid=R3D5NBmh4C0ecNjTprCgF0&origin_space=116&toggle=vis
SELECT
    (CASE 
        WHEN (dashboard.preferred_viewer != 'dashboards-next') OR (dashboard.preferred_viewer IS NULL)  
        THEN 'Yes' 
        ELSE 'No' 
        END
    ) AS `dashboard.is_legacy`,
    dashboard.id AS `dashboard.id`,
    dashboard.TITLE  AS `dashboard.title`,
    COUNT(DISTINCT history.dashboard_session) AS `history.dashboard_run_count`
FROM 
    history
    LEFT JOIN dashboard ON history.dashboard_id = dashboard.id
    LEFT JOIN user_facts ON history.user_id = user_facts.user_id
WHERE 
    ((
        (history.COMPLETED_AT) >= ((DATE_ADD(CURDATE(),INTERVAL -30 day))) 
        AND (history.COMPLETED_AT) < ((DATE_ADD(DATE_ADD(CURDATE(),INTERVAL -30 day),INTERVAL 30 day)))
    )) 
    AND (dashboard.id) IN (26, 27, 28, 30, 32, 34, 35, 70, 71) 
    AND (
        NOT (user_facts.is_verified_looker_employee ) 
        OR (user_facts.is_verified_looker_employee ) IS NULL
    ) 
    AND (user_facts.last_ui_login_credential_type) = 'embed'
GROUP BY
    1,
    2,
    3
ORDER BY
    4 DESC