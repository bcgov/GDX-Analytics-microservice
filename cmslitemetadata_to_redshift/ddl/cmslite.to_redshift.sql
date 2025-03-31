-- perform this as a transaction.
-- Either the whole query completes, or it leaves the old table intact
BEGIN;
SET search_path TO {dbschema};
DROP TABLE IF EXISTS {dbschema}.themes;
CREATE TABLE IF NOT EXISTS {dbschema}.themes (
"node_id"        VARCHAR(255)  ENCODE RAW,
"title"          VARCHAR(2047) ENCODE LZO,
"hr_url"         VARCHAR(2047) ENCODE LZO,
"parent_node_id" VARCHAR(255)  ENCODE LZO,
"parent_title"   VARCHAR(2047) ENCODE LZO,
"theme_id"       VARCHAR(255)  ENCODE LZO,
"subtheme_id"    VARCHAR(255)  ENCODE LZO,
"topic_id"       VARCHAR(255)  ENCODE LZO,
"subtopic_id"    VARCHAR(255)  ENCODE LZO,
"subsubtopic_id" VARCHAR(255)  ENCODE LZO,
"theme"          VARCHAR(2047) ENCODE LZO,
"subtheme"       VARCHAR(2047) ENCODE LZO,
"topic"          VARCHAR(2047) ENCODE LZO,
"subtopic"       VARCHAR(2047) ENCODE LZO,
"subsubtopic"    VARCHAR(2047) ENCODE LZO,
"full_tree_nodes" VARCHAR(2047) ENCODE LZO
)
DISTSTYLE AUTO
SORTKEY ( node_id );
ALTER TABLE {dbschema}.themes OWNER TO microservice;
GRANT SELECT ON {dbschema}.themes TO looker;

INSERT INTO {dbschema}.themes
WITH ids
AS (SELECT cm.node_id,
cm.title,
cm.hr_url,
cm.parent_node_id,
cm_parent.title AS parent_title,
cm.ancestor_nodes,
CASE
-- page is root: Gov, Intranet, ALC, MCFD or Training SITE
WHEN cm.node_id IN ('CA4CBBBB070F043ACF7FB35FE3FD1081',
		    'A9A4B738CE26466C92B45A66DD8C2AFC',
		    '7B239105652B4EBDAB215C59B75A453B',
		    'AFE735F4ADA542ACA830EBC10D179FBE',
		    'D69135AB037140D880A4B0E725D15774')
  THEN '||'
-- parent page is root: Gov, Intranet, ALC, MCFD or Training SITE
WHEN cm.parent_node_id IN ('CA4CBBBB070F043ACF7FB35FE3FD1081',
		    'A9A4B738CE26466C92B45A66DD8C2AFC',
		    '7B239105652B4EBDAB215C59B75A453B',
		    'AFE735F4ADA542ACA830EBC10D179FBE',
		    'D69135AB037140D880A4B0E725D15774')
  THEN '|' || cm.node_id || '|'
-- "first" page is root: Gov, Intranet, ALC, MCFD or Training SITE
WHEN TRIM(SPLIT_PART(cm.ancestor_nodes, '|', 2)) IN
		   ('CA4CBBBB070F043ACF7FB35FE3FD1081',
		    'A9A4B738CE26466C92B45A66DD8C2AFC',
		    '7B239105652B4EBDAB215C59B75A453B',
		    'AFE735F4ADA542ACA830EBC10D179FBE',
		    'D69135AB037140D880A4B0E725D15774')
  THEN REPLACE(cm.ancestor_nodes, '|' ||
    TRIM(SPLIT_PART(cm.ancestor_nodes, '|', 2)), '') ||
    cm.parent_node_id || '|' || cm.node_id || '|'
-- an exception for assets, push the parent node to level2 and
-- leave the node out of the hierarchy
WHEN cm.ancestor_nodes = '||' AND cm.page_type = 'ASSET'
  THEN cm.ancestor_nodes || cm.parent_node_id
-- no ancestor nodes
WHEN cm.ancestor_nodes = '||'
  THEN '|' || cm.parent_node_id || '|' || cm.node_id || '|'
ELSE cm.ancestor_nodes || cm.parent_node_id || '|' || cm.node_id || '|'
END AS full_tree_nodes,
-- The first SPLIT_PART of full_tree_nodes is always blank as the
-- string has '|' on each end
CASE
WHEN TRIM(SPLIT_PART(full_tree_nodes, '|', 2)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 2))
ELSE NULL
END AS level1_id,
CASE
WHEN TRIM(SPLIT_PART(full_tree_nodes, '|', 3)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 3))
ELSE NULL
END AS level2_id,
--  exception for Service BC pages:
-- "promote" FD6DB5BA2A5248038EEF54D9F9F37C4D as a topic and
-- raise up its children as sub-topics
CASE
WHEN TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) =
  'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  THEN 'FD6DB5BA2A5248038EEF54D9F9F37C4D'
WHEN TRIM(SPLIT_PART(full_tree_nodes, '|', 4)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 4))
ELSE NULL
END AS level3_id,
CASE
WHEN
TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) =
'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  AND TRIM(SPLIT_PART(full_tree_nodes, '|', 8)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 8))
WHEN
TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) <>
'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  AND TRIM(SPLIT_PART(full_tree_nodes, '|', 5)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 5))
ELSE NULL
END AS level4_id,
CASE
WHEN
TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) =
'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  AND TRIM(SPLIT_PART(full_tree_nodes, '|', 9)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 9))
WHEN
TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) <>
'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  AND TRIM(SPLIT_PART(full_tree_nodes, '|', 6)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 6))
ELSE NULL
END AS level5_id
FROM {dbschema}.metadata AS cm
LEFT JOIN {dbschema}.metadata AS cm_parent
ON cm_parent.node_id = cm.parent_node_id
WHERE cm.page_type NOT LIKE 'ASSET_FOLDER'
AND cm.page_type NOT LIKE 'ASSET'),
biglist
AS (SELECT
ROW_NUMBER () OVER ( PARTITION BY ids.node_id ) AS index,
ids.*,
CASE 
WHEN l1.page_type = 'ASSET'
THEN NULL
ELSE l1.title
END AS theme,
l2.title AS subtheme,
l3.title AS topic,
l4.title AS subtopic,
l5.title AS subsubtopic,
CASE
WHEN theme IS NOT NULL
THEN level1_ID
ELSE NULL
END AS theme_ID,
CASE
WHEN subtheme IS NOT NULL
THEN level2_ID
ELSE NULL
END AS subtheme_ID,
CASE
WHEN topic IS NOT NULL
THEN level3_ID
ELSE NULL
END AS topic_ID,
CASE
WHEN subtopic IS NOT NULL
THEN level4_ID
ELSE NULL
END AS subtopic_ID,
CASE
WHEN subsubtopic IS NOT NULL
THEN level5_ID
ELSE NULL
END AS subsubtopic_ID
FROM ids
LEFT JOIN {dbschema}.metadata AS l1
ON l1.node_id = ids.level1_id
LEFT JOIN {dbschema}.metadata AS l2
ON l2.node_id = ids.level2_id
LEFT JOIN {dbschema}.metadata AS l3
ON l3.node_id = ids.level3_id
LEFT JOIN {dbschema}.metadata AS l4
ON l4.node_id = ids.level4_id
LEFT JOIN {dbschema}.metadata AS l5
ON l5.node_id = ids.level5_id
)
SELECT node_id,
title,
hr_url,
parent_node_id,
parent_title,
theme_id,
subtheme_id,
topic_id,
subtopic_id,
subsubtopic_id,
theme,
subtheme,
topic,
subtopic,
subsubtopic,
full_tree_nodes
FROM biglist
WHERE index = 1;
--- fix for https://www2.gov.bc.ca/getvaccinated.html (note that there are two extra entries for this one)
INSERT INTO {dbschema}.metadata (
SELECT 'A2DB016A552E4D3DAD0832B264700000' AS node_id,parent_node_id,ancestor_nodes, hr_url,
      keywords,description,page_type,folder_name,synonyms,dcterms_creator,modified_date,created_date,updated_date,published_date,title,nav_title,
      eng_nav_title,sitekey,site_id,language_name,language_code,page_status,published_by,created_by,modified_by,node_level,
      locked_date,moved_date,exclude_from_ia,hide_from_navigation,exclude_from_search_engines,security_classification,security_label,
      publication_date,defined_security_groups,inherited_security_groups
FROM {dbschema}.metadata WHERE node_id = 'A2DB016A552E4D3DAD0832B26472BA8E'
);
INSERT INTO {dbschema}.metadata (
SELECT 'A2DB016A552E4D3DAD0832B264700005' AS node_id,parent_node_id,ancestor_nodes, hr_url,
      keywords,description,page_type,folder_name,synonyms,dcterms_creator,modified_date,created_date,updated_date,published_date,title,nav_title,
      eng_nav_title,sitekey,site_id,language_name,language_code,page_status,published_by,created_by,modified_by,node_level,
      locked_date,moved_date,exclude_from_ia,hide_from_navigation,exclude_from_search_engines,security_classification,security_label,
      publication_date,defined_security_groups,inherited_security_groups
FROM {dbschema}.metadata WHERE node_id = 'A2DB016A552E4D3DAD0832B26472BA8E'
);
INSERT INTO {dbschema}.themes (
SELECT 'A2DB016A552E4D3DAD0832B264700000' AS node_id, title, hr_url, parent_node_id, 
      parent_title, theme_id, subtheme_id, topic_id, subtopic_id, subsubtopic_id, theme, subtheme, topic, subtopic, subsubtopic 
FROM {dbschema}.themes WHERE node_id  = 'A2DB016A552E4D3DAD0832B26472BA8E'
);
INSERT INTO {dbschema}.themes (
SELECT 'A2DB016A552E4D3DAD0832B264700005' AS node_id, title, hr_url, parent_node_id, 
      parent_title, theme_id, subtheme_id, topic_id, subtopic_id, subsubtopic_id, theme, subtheme, topic, subtopic, subsubtopic 
FROM {dbschema}.themes WHERE node_id  = 'A2DB016A552E4D3DAD0832B26472BA8E'
);
--- fix for https://www2.gov.bc.ca/vaccinecard.html
INSERT INTO {dbschema}.metadata (
SELECT '465BA70BBD2441D2A79F06B490700000' AS node_id,parent_node_id,ancestor_nodes, hr_url,
      keywords,description,page_type,folder_name,synonyms,dcterms_creator,modified_date,created_date,updated_date,published_date,title,nav_title,
      eng_nav_title,sitekey,site_id,language_name,language_code,page_status,published_by,created_by,modified_by,node_level,
      locked_date,moved_date,exclude_from_ia,hide_from_navigation,exclude_from_search_engines,security_classification,security_label,
      publication_date,defined_security_groups,inherited_security_groups
FROM {dbschema}.metadata WHERE node_id = '465BA70BBD2441D2A79F06B4907118C5'
);
INSERT INTO {dbschema}.themes (
SELECT '465BA70BBD2441D2A79F06B490700000' AS node_id, title, hr_url, parent_node_id, 
      parent_title, theme_id, subtheme_id, topic_id, subtopic_id, subsubtopic_id, theme, subtheme, topic, subtopic, subsubtopic 
FROM {dbschema}.themes WHERE node_id  = '465BA70BBD2441D2A79F06B4907118C5'
);
UPDATE {dbschema}.metadata 
SET folder_name = l2.title 
FROM {dbschema}.metadata as l1 
INNER JOIN {dbschema}.metadata as l2 ON l1.parent_node_id = l2.node_id 
WHERE l1.parent_node_id in (select node_id from {dbschema}.metadata where page_type like 'ASSET_FOLDER');

DROP TABLE IF EXISTS {dbschema}.asset_themes;
CREATE TABLE IF NOT EXISTS {dbschema}.asset_themes (
"node_id"	       VARCHAR(255) ENCODE ZSTD,
"title"		   VARCHAR(2047) ENCODE ZSTD,
"hr_url"	       VARCHAR(2047) ENCODE ZSTD,
"parent_node_id" VARCHAR(255) ENCODE ZSTD,
"parent_title"   VARCHAR(2047) ENCODE ZSTD,
"asset_theme_id"	   VARCHAR(255) ENCODE ZSTD,
"asset_subtheme_id"	   VARCHAR(255) ENCODE ZSTD,
"asset_topic_id"	   VARCHAR(255) ENCODE ZSTD,
"asset_subtopic_id"	   VARCHAR(255) ENCODE ZSTD,
"asset_subsubtopic_id" VARCHAR(255) ENCODE ZSTD,
"asset_theme"		   VARCHAR(2047) ENCODE ZSTD,
"asset_subtheme"	   VARCHAR(2047) ENCODE ZSTD,
"asset_topic"		   VARCHAR(2047) ENCODE ZSTD,
"asset_subtopic"	   VARCHAR(2047) ENCODE ZSTD,
"asset_subsubtopic"	   VARCHAR(2047) ENCODE ZSTD,
"full_tree_nodes"     VARCHAR(2047) ENCODE ZSTD,
"sitekey"           VARCHAR(20) ENCODE ZSTD
);
ALTER TABLE {dbschema}.asset_themes OWNER TO microservice;
GRANT SELECT ON {dbschema}.asset_themes TO looker;

INSERT INTO {dbschema}.asset_themes
WITH ids
AS (SELECT cm.node_id,
cm.title,
cm.hr_url,
cm.parent_node_id,
cm_parent.title AS parent_title,
CASE 
WHEN cm.page_type LIKE 'ASSET' AND cm_parent.ancestor_nodes LIKE '||' 
    THEN '|' || cm_parent.parent_node_id || '|'
WHEN cm.page_type LIKE 'ASSET' AND cm_parent.ancestor_nodes LIKE ''
    THEN cm_parent.ancestor_nodes || cm_parent.parent_node_id || '|'
 WHEN cm.page_type LIKE 'ASSET' AND cm_parent.ancestor_nodes NOT LIKE '' AND cm_parent.ancestor_nodes NOT LIKE '||'
    THEN cm_parent.ancestor_nodes || cm_parent.parent_node_id || '|'
ELSE cm.ancestor_nodes
END AS ancestor_folders,
CASE
-- page is root: Gov, Intranet, ALC, MCFD or Training SITE
WHEN cm.node_id IN ('CA4CBBBB070F043ACF7FB35FE3FD1081',
		    'A9A4B738CE26466C92B45A66DD8C2AFC',
		    '7B239105652B4EBDAB215C59B75A453B',
		    'AFE735F4ADA542ACA830EBC10D179FBE',
		    'D69135AB037140D880A4B0E725D15774')
  THEN '||'
-- parent page is root: Gov, Intranet, ALC, MCFD or Training SITE
WHEN cm.parent_node_id IN ('CA4CBBBB070F043ACF7FB35FE3FD1081',
		    'A9A4B738CE26466C92B45A66DD8C2AFC',
		    '7B239105652B4EBDAB215C59B75A453B',
		    'AFE735F4ADA542ACA830EBC10D179FBE',
		    'D69135AB037140D880A4B0E725D15774')
  THEN '|' || cm.node_id || '|'
-- "first" page is root: Gov, Intranet, ALC, MCFD or Training SITE
WHEN TRIM(SPLIT_PART(ancestor_folders, '|', 2)) IN
		   ('CA4CBBBB070F043ACF7FB35FE3FD1081',
		    'A9A4B738CE26466C92B45A66DD8C2AFC',
		    '7B239105652B4EBDAB215C59B75A453B',
		    'AFE735F4ADA542ACA830EBC10D179FBE',
		    'D69135AB037140D880A4B0E725D15774')
  THEN REPLACE(ancestor_folders, '|' ||
    TRIM(SPLIT_PART(ancestor_folders, '|', 2)), '') ||
    cm.parent_node_id || '|' || cm.node_id || '|'
WHEN ancestor_folders = '||' 
  THEN '|' || cm.parent_node_id || '|' || cm.node_id || '|'
ELSE ancestor_folders || cm.parent_node_id || '|'
END AS full_tree_nodes,
-- The first SPLIT_PART of full_tree_nodes is always blank as the
-- string has '|' on each end
CASE
WHEN TRIM(SPLIT_PART(full_tree_nodes, '|', 2)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 2))
ELSE NULL
END AS level1_id,
CASE
WHEN TRIM(SPLIT_PART(full_tree_nodes, '|', 3)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 3))
ELSE NULL
END AS level2_id,
--  exception for Service BC pages:
-- "promote" FD6DB5BA2A5248038EEF54D9F9F37C4D as a topic and
-- raise up its children as sub-topics
CASE
WHEN TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) =
  'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  THEN 'FD6DB5BA2A5248038EEF54D9F9F37C4D'
WHEN TRIM(SPLIT_PART(full_tree_nodes, '|', 4)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 4))
ELSE NULL
END AS level3_id,
CASE
WHEN
TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) =
'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  AND TRIM(SPLIT_PART(full_tree_nodes, '|', 8)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 8))
WHEN
TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) <>
'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  AND TRIM(SPLIT_PART(full_tree_nodes, '|', 5)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 5))
ELSE NULL
END AS level4_id,
CASE
WHEN
TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) =
'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  AND TRIM(SPLIT_PART(full_tree_nodes, '|', 9)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 9))
WHEN
TRIM(SPLIT_PART(full_tree_nodes, '|', 7)) <>
'FD6DB5BA2A5248038EEF54D9F9F37C4D'
  AND TRIM(SPLIT_PART(full_tree_nodes, '|', 6)) <> ''
  THEN TRIM(SPLIT_PART(full_tree_nodes, '|', 6))
ELSE NULL
END AS level5_id
FROM {dbschema}.metadata AS cm
LEFT JOIN {dbschema}.metadata AS cm_parent
ON cm_parent.node_id = cm.parent_node_id
WHERE cm.page_type like 'ASSET_FOLDER'
OR cm.page_type LIKE 'ASSET'),
biglist
AS (SELECT
ROW_NUMBER () OVER ( PARTITION BY ids.node_id ) AS index,
ids.*,
l1.title AS asset_theme,
l2.title AS asset_subtheme,
l3.title AS asset_topic,
l4.title AS asset_subtopic,
l5.title AS asset_subsubtopic,
CASE
WHEN asset_theme IS NOT NULL
THEN level1_ID
ELSE NULL
END AS asset_theme_ID,
CASE
WHEN asset_subtheme IS NOT NULL
THEN level2_ID
ELSE NULL
END AS asset_subtheme_ID,
CASE
WHEN asset_topic IS NOT NULL
THEN level3_ID
ELSE NULL
END AS asset_topic_ID,
CASE
WHEN asset_subtopic IS NOT NULL
THEN level4_ID
ELSE NULL
END AS asset_subtopic_ID,
CASE
WHEN asset_subsubtopic IS NOT NULL
THEN level5_ID
ELSE NULL
END AS asset_subsubtopic_ID,
l1.sitekey as sitekey
FROM ids
LEFT JOIN {dbschema}.metadata AS l1
ON l1.node_id = ids.level1_id
LEFT JOIN {dbschema}.metadata AS l2
ON l2.node_id = ids.level2_id
LEFT JOIN {dbschema}.metadata AS l3
ON l3.node_id = ids.level3_id
LEFT JOIN {dbschema}.metadata AS l4
ON l4.node_id = ids.level4_id
LEFT JOIN {dbschema}.metadata AS l5
ON l5.node_id = ids.level5_id
)
SELECT node_id,
title,
hr_url,
parent_node_id,
parent_title,
asset_theme_id,
asset_subtheme_id,
asset_topic_id,
asset_subtopic_id,
asset_subsubtopic_id,
asset_theme,
asset_subtheme,
asset_topic,
asset_subtopic,
asset_subsubtopic,
full_tree_nodes,
sitekey
FROM biglist
WHERE index = 1;

ANALYZE {dbschema}.asset_themes;

