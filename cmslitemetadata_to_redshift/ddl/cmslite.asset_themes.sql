CREATE TABLE IF NOT EXISTS cmslite.asset_themes (
	"node_id"	VARCHAR(255),
	"title"		VARCHAR(2047),
	"hr_url"	VARCHAR(2047),
	"parent_node_id" VARCHAR(255),
	"parent_title"	VARCHAR(2047),
	"asset_theme_id"	VARCHAR(255),
	"asset_subtheme_id"	VARCHAR(255),
	"asset_topic_id"	VARCHAR(255),
	"asset_subtopic_id"	VARCHAR(255),
	"asset_subsubtopic_id" VARCHAR(255),
	"asset_theme"		VARCHAR(2047),
	"asset_subtheme"	VARCHAR(2047),
	"asset_topic"		VARCHAR(2047),
	"asset_subtopic"	VARCHAR(2047),
	"asset_subsubtopic"	VARCHAR(2047),
	"sitekey"			VARCHAR(20)
);      
ALTER TABLE cmslite.asset_themes OWNER TO microservice;
GRANT SELECT ON cmslite.asset_themes TO looker;
