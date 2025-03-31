CREATE TABLE IF NOT EXISTS cmslite.themes (
	node_id         VARCHAR(255)  ENCODE RAW,
	title           VARCHAR(2047) ENCODE LZO,
	hr_url          VARCHAR(2047) ENCODE LZO,
	parent_node_id  VARCHAR(255)  ENCODE LZO,
	parent_title    VARCHAR(2047) ENCODE LZO,
	theme_id        VARCHAR(255)  ENCODE LZO,
	subtheme_id     VARCHAR(255)  ENCODE LZO,
	topic_id        VARCHAR(255)  ENCODE LZO,
	subtopic_id     VARCHAR(255)  ENCODE LZO,
	subsubtopic_id  VARCHAR(255)  ENCODE LZO,
	theme           VARCHAR(2047) ENCODE LZO,
	subtheme        VARCHAR(2047) ENCODE LZO,
	topic           VARCHAR(2047) ENCODE LZO,
	subtopic        VARCHAR(2047) ENCODE LZO,
	subsubtopic     VARCHAR(2047) ENCODE LZO,
	full_tree_nodes VARCHAR(2047) ENCODE LZO

)
DISTKEY AUTO
SORTKEY ( node_id );
ALTER TABLE cmslite.themes OWNER TO microservice;
GRANT SELECT ON cmslite.themes TO looker;
