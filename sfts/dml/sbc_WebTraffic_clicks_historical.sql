SELECT * FROM (
  SELECT
      clicks.target_url  AS "clicks.target_url",
          (CASE WHEN clicks.is_government THEN ''Yes'' ELSE ''No'' END) AS "clicks.is_government",
          (CASE WHEN clicks.offsite_click  THEN ''Yes'' ELSE ''No'' END) AS "clicks.offsite_click",
      clicks.click_type  AS "clicks.click_type",
      clicks.session_id  AS "clicks.session_id",
          (TO_CHAR(DATE_TRUNC(''second'', clicks.collector_tstamp ), ''YYYY-MM-DD HH24:MI:SS'')) AS "clicks.click_time_time"
  FROM derived.clicks  AS clicks
  LEFT JOIN cmslite.themes  AS cmslite_themes ON clicks.node_id = cmslite_themes.node_id
  WHERE (clicks.collector_tstamp ) < ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND (COALESCE(cmslite_themes.topic, ''(no topic)'') ) = ''Service BC'' AND ((clicks.node_id) IS NOT NULL AND (clicks.page_urlhost ) IS NOT NULL) AND ((clicks.page_exclusion_filter) IS NOT NULL AND (clicks.app_id ) IS NOT NULL AND ((clicks.page_section) IS NOT NULL AND ((clicks.page_subsection ) IS NOT NULL AND (COALESCE(cmslite_themes.theme_id,'''') ) IS NOT NULL)))
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  ORDER BY
      6
)
