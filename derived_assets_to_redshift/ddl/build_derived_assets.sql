BEGIN;
SET SEARCH_PATH TO '{schema_name}';
INSERT INTO asset_downloads_derived (
SELECT LEFT ('{asset_scheme_and_authority}' || 
    SPLIT_PART(assets.request_string, ' ',2), 4093)
    AS asset_url,
assets.date_timestamp::TIMESTAMP,
assets.ip AS ip_address,
assets.request_response_time,
assets.referrer,
assets.return_size,
assets.status_code,
-- strip down the asset_url by removing host, query, etc,
-- then use a regex to get the filename from the remaining path.
REGEXP_SUBSTR(
    REGEXP_REPLACE(
      SPLIT_PART(
	SPLIT_PART(
	  SPLIT_PART(
	    asset_url, '{asset_host}' , 2),
	  '?', 1),
	'#', 1),
      '(.aspx)$'),
'([^\/]+\.[A-Za-z0-9]+)$') AS asset_file,
-- strip down the asset_url by removing host, query, etc, then use
-- a regex to get the file extension from the remaining path.
CASE
  WHEN SPLIT_PART(
    REGEXP_REPLACE(
      SPLIT_PART(
	SPLIT_PART(
	  SPLIT_PART(
	    asset_url, '%', 1),
	  '?', 1),
	'#', 1),
      '(.aspx)$'),
    '{asset_host}', 2) LIKE '%.%'
  THEN REGEXP_SUBSTR(
    SPLIT_PART(
      REGEXP_REPLACE(
	SPLIT_PART(
	  SPLIT_PART(
	    SPLIT_PART(
		asset_url, '%', 1),
	    '?', 1),
	  '#', 1),
	'(.aspx)$'),
      '{asset_host}', 2),
    '([^\.]+$)')
  ELSE NULL
END AS asset_ext,
assets.user_agent_http_request_header,
assets.request_string,
CASE
    WHEN request_string LIKE '%/assets/download/%' AND referrer LIKE '%mcfd%' THEN 'mcfd'
    ELSE '{asset_host}'
END AS asset_host,
'{asset_source}' as asset_source,
CASE
    WHEN assets.referrer is NULL THEN TRUE
    ELSE FALSE
    END AS direct_download,
CASE
    WHEN
	REGEXP_SUBSTR(assets.referrer, '[^/]+\\\.[^/:]+')
	<> '{asset_host}'
    THEN TRUE
    ELSE FALSE
    END AS offsite_download,
CASE
    WHEN assets.ip LIKE '184.69.13.%'
    OR assets.ip LIKE '184.71.25.%' THEN TRUE
    ELSE FALSE
    END AS is_efficiencybc_dev,
CASE WHEN assets.ip LIKE '142.22.%'
    OR assets.ip LIKE '142.23.%'
    OR assets.ip LIKE '142.24.%'
    OR assets.ip LIKE '142.25.%'
    OR assets.ip LIKE '142.26.%'
    OR assets.ip LIKE '142.27.%'
    OR assets.ip LIKE '142.28.%'
    OR assets.ip LIKE '142.29.%'
    OR assets.ip LIKE '142.30.%'
    OR assets.ip LIKE '142.31.%'
    OR assets.ip LIKE '142.32.%'
    OR assets.ip LIKE '142.33.%'
    OR assets.ip LIKE '142.34.%'
    OR assets.ip LIKE '142.35.%'
    OR assets.ip LIKE '142.36.%'
    THEN TRUE
    ELSE FALSE
    END AS is_government,
CASE WHEN assets.user_agent_http_request_header LIKE '%Mobile%'
    THEN TRUE
    ELSE FALSE
    END AS is_mobile,
CASE
    WHEN assets.user_agent_http_request_header
	LIKE '%Mobile%' THEN 'Mobile'
    WHEN assets.user_agent_http_request_header
	LIKE '%Tablet%' THEN 'Tablet'
    WHEN assets.user_agent_http_request_header
	ILIKE '%neo-x%' THEN 'Digital media receiver'
    WHEN assets.user_agent_http_request_header ILIKE '%playstation%'
	OR  assets.user_agent_http_request_header ILIKE '%nintendo%'
	OR  assets.user_agent_http_request_header ILIKE '%xbox%'
	THEN 'Game Console'
    WHEN assets.user_agent_http_request_header LIKE '%Macintosh%'
	OR assets.user_agent_http_request_header LIKE '%Windows NT%'
	THEN 'Computer'
    ELSE 'Unknown'
            END AS device,
        assets.os_family,
        assets.os_version,
        assets.browser_family,
        assets.browser_version,
        -- Redshift requires the two extra escaping slashes for the
        -- backslash in the regex for referrer_urlhost.
        REGEXP_SUBSTR(assets.referrer, '[^/]+\\\.[^/:]+')
        AS referrer_urlhost_derived,
        assets.referrer_medium,
        SPLIT_PART(
            SPLIT_PART(
                REGEXP_SUBSTR(
                    REGEXP_REPLACE(assets.referrer,'.*:\/\/'), '/.*'), '?', 1),
                    '#', 1)
        AS referrer_urlpath,
        CASE
            WHEN POSITION ('?' IN referrer) > 0
            THEN SUBSTRING (referrer,
                            POSITION ('?' IN referrer) +1)
            ELSE ''
            END AS referrer_urlquery,
        SPLIT_PART(assets.referrer, ':', 1) AS referrer_urlscheme,
        CASE
            WHEN referrer_urlhost_derived = 'www2.gov.bc.ca'
                AND referrer_urlpath = '/gov/search'
            THEN 'https://www2.gov.bc.ca/gov/search?' || referrer_urlquery
            WHEN referrer_urlhost_derived = 'www2.gov.bc.ca'
                AND referrer_urlpath = '/enSearch/sbcdetail'
            THEN 'https://www2.gov.bc.ca/enSearch/sbcdetail?' ||
                REGEXP_REPLACE(referrer_urlquery,'([^&]*&[^&]*)&.*','$1')
            WHEN referrer_urlpath IN (
                '/solutionexplorer/ES_Access',
                '/solutionexplorer/ES_Question',
                '/solutionexplorer/ES_Result',
                '/solutionexplorer/ES_Action')
                AND LEFT(referrer_urlquery, 3) = 'id='
            THEN referrer_urlscheme || '://' || referrer_urlhost_derived  ||
                referrer_urlpath ||'?' ||
                SPLIT_PART(referrer_urlquery,'&',1)
            ELSE referrer_urlscheme || '://' || referrer_urlhost_derived  ||
                REGEXP_REPLACE(
                    referrer_urlpath,
                    'index.(html|htm|aspx|php|cgi|shtml|shtm)$','')
            END AS page_referrer_display_url,
        LOWER(asset_url) AS asset_url_case_insensitive,
        REGEXP_REPLACE(asset_url, '\\?.*$') AS asset_url_nopar,
        LOWER(
            REGEXP_REPLACE(asset_url, '\\?.*$'))
        AS asset_url_nopar_case_insensitive,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    asset_url_nopar_case_insensitive,
                    '/((index|default)\\.(htm|html|cgi|shtml|shtm))|(default\\.(asp|aspx))/{{0,}}$','/'),
                '//$','/'),
            '%20',' ')
        AS truncated_asset_url_nopar_case_insensitive
         FROM {schema_name}.asset_downloads AS assets
        -- Asset files not in the getmedia folder for TIBC and
        -- workbc must be filtered out
       WHERE '{asset_scheme_and_authority}' NOT IN (
            'https://www.workbc.ca',
            'https://www.britishcolumbia.ca')
        OR (request_string LIKE '%getmedia%'
            AND asset_url LIKE 'https://www.workbc.ca%')
        OR (request_string LIKE '%wp-content/uploads%' 
            AND asset_source LIKE 'TIBC')
    );
    {truncate_intermediate_table}
    COMMIT;
