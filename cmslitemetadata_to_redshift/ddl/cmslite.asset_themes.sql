CREATE TABLE IF NOT EXISTS cmslite.asset_themes (
	"node_id"	VARCHAR(255) ENCODE ZSTD,
	"title"		VARCHAR(2047) ENCODE ZSTD,
	"hr_url"	VARCHAR(2047) ENCODE ZSTD,
	"parent_node_id" VARCHAR(255) ENCODE ZSTD,
	"parent_title"	VARCHAR(2047) ENCODE ZSTD,
	"asset_theme_id"	VARCHAR(255) ENCODE ZSTD,
	"asset_subtheme_id"	VARCHAR(255) ENCODE ZSTD,
	"asset_topic_id"	VARCHAR(255) ENCODE ZSTD,
	"asset_subtopic_id"	VARCHAR(255) ENCODE ZSTD,
	"asset_subsubtopic_id" VARCHAR(255) ENCODE ZSTD,
	"asset_theme"		VARCHAR(2047) ENCODE ZSTD,
	"asset_subtheme"	VARCHAR(2047) ENCODE ZSTD,
	"asset_topic"		VARCHAR(2047) ENCODE ZSTD,
	"asset_subtopic"	VARCHAR(2047) ENCODE ZSTD,
	"asset_subsubtopic"	VARCHAR(2047) ENCODE ZSTD,
	"full_tree_nodes"	VARCHAR(2047) ENCODE ZSTD,
	"sitekey"			VARCHAR(20) ENCODE ZSTD
);      
ALTER TABLE cmslite.asset_themes OWNER TO microservice;
GRANT SELECT ON cmslite.asset_themes TO looker;
