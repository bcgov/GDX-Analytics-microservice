SELECT * FROM (
-- based on the SQL runner: https://analytics.gov.bc.ca/sql/x6h848vfy8rtcr
  SELECT
      (DATE("asset_downloads"."date_timestamp")) AS "asset_downloads.download_time_date",
      "asset_downloads"."asset_file" AS "asset_downloads.asset_file",
      SPLIT_PART(SPLIT_PART(asset_downloads.asset_url,''?'', 1), ''#'', 1)  AS "asset_downloads.asset_display_url",
      COUNT(*) AS "asset_downloads.count"
  FROM
      "microservice"."asset_downloads_derived" AS "asset_downloads"
      LEFT JOIN "cmslite"."asset_themes" AS "asset_themes" ON "asset_downloads"."asset_url_nopar_case_insensitive" = "asset_themes"."hr_url"
  WHERE ((( "asset_downloads"."date_timestamp" ) >= ((DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ))) AND ( "asset_downloads"."date_timestamp" ) < ((DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC(''month'', DATE_TRUNC(''day'',GETDATE())) ) ))))) AND (COALESCE(asset_themes.asset_subtheme_id,'''') ) IN (''5421FABE8E0C4C3BA9E6AE124D6CAA8B'', ''B12563FDE5984CFB9E4442734AAB0FC0'') AND (COALESCE(asset_themes.asset_theme_id,'''') ) IS NOT NULL AND "asset_downloads"."asset_host" IS NOT NULL AND (SPLIT_PART(SPLIT_PART(asset_downloads.asset_url,''?'', 1), ''#'', 1) ) IS NOT NULL
  GROUP BY
      1,
      2,
      3
  ORDER BY
      1
)