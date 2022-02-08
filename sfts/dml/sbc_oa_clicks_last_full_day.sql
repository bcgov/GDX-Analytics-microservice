SELECT * FROM
(
  SELECT
  ac.root_id, wp.id AS page_view_id,
  pv.session_id,
  label,
  logged_in,
  appointment_step,
  location,
  service,
  url,
  CASE WHEN label = ''Login: BCeID'' THEN 1 ELSE 0 END AS bceid_login_count,
  CASE WHEN label = ''Create: BCeID'' THEN 1 ELSE 0 END AS bceid_create_count,
  CASE WHEN label = ''Register'' THEN 1 ELSE 0 END AS register_count,
  CASE WHEN label = ''Info: Privacy Statement'' THEN 1 ELSE 0 END AS privacy_count,
  CASE WHEN label = ''Help'' THEN 1 ELSE 0 END AS help_count,
  CASE WHEN label = ''Online Option'' THEN 1 ELSE 0 END AS online_option_count,
  CASE WHEN label = ''Login'' THEN 1 ELSE 0 END AS login_count,
  CASE WHEN label = ''View Location Services'' THEN 1 ELSE 0 END AS location_services_count,
  CASE WHEN label = ''Login: BC Services Card'' THEN 1 ELSE 0 END AS bcsc_login_count,
  CASE WHEN label = ''Info: About the BCeID'' THEN 1 ELSE 0 END AS bceid_info_count,
  CASE WHEN label NOT IN (
    ''Login: BCeID'',
    ''Create: BCeID'',
    ''Register'',
    ''Info: Privacy Statement'',
    ''Help'',
    ''Online Option'',
    ''Login'',
    ''View Location Services'',
    ''Login: BC Services Card'',
    ''Info: About the BCeID''
    ) THEN 1 ELSE 0 END AS other_count,
  CONVERT_TIMEZONE(''UTC'', ''America/Vancouver'', ac.root_tstamp) AS timestamp
FROM atomic.ca_bc_gov_cfmspoc_appointment_click_1 AS ac
  JOIN atomic.com_snowplowanalytics_snowplow_web_page_1 AS wp
      ON ac.root_id = wp.root_id AND ac.root_tstamp = wp.root_tstamp
  JOIN derived.page_views AS pv ON pv.page_view_id = wp.id
WHERE
  timestamp > DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) AND
  timestamp < DATE_TRUNC(''day'',GETDATE())
)
