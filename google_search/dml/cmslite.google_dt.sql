-- perform this as a transaction.
-- Either the whole query completes, or it leaves the old table intact
BEGIN;
DROP TABLE IF EXISTS cmslite.google_dt;
CREATE TABLE IF NOT EXISTS cmslite.google_dt (
        site              VARCHAR(255)    ENCODE ZSTD,
        date              date            ,
        query             VARCHAR(2048)   ENCODE ZSTD,
        country           VARCHAR(255)    ENCODE ZSTD,
        device            VARCHAR(255)    ENCODE ZSTD,
        page              VARCHAR(2047)   ENCODE ZSTD,
        position          FLOAT           ENCODE ZSTD,
        clicks            DECIMAL         ENCODE AZ64,
        ctr               FLOAT           ENCODE ZSTD,
        impressions       DECIMAL         ENCODE AZ64,
        node_id           VARCHAR(255)    ENCODE ZSTD,
        page_urlhost      VARCHAR(255)    ENCODE ZSTD,
        title             VARCHAR(2047)   ENCODE ZSTD,
        theme_id          VARCHAR(255)    ENCODE ZSTD,
        subtheme_id       VARCHAR(255)    ENCODE ZSTD,
        topic_id          VARCHAR(255)    ENCODE ZSTD,
        subtopic_id       VARCHAR(255)    ENCODE ZSTD,
        subsubtopic_id    VARCHAR(255)    ENCODE ZSTD,
        theme             VARCHAR(2047)   ENCODE ZSTD,
        subtheme          VARCHAR(2047)   ENCODE ZSTD,
        topic             VARCHAR(2047)   ENCODE ZSTD,
        subtopic          VARCHAR(2047)   ENCODE ZSTD,
        subsubtopic       VARCHAR(2047)   ENCODE ZSTD)
        COMPOUND SORTKEY (date,page_urlhost,theme,page,clicks);

ALTER TABLE cmslite.google_dt OWNER TO microservice;
GRANT SELECT ON cmslite.google_dt TO looker;

INSERT INTO cmslite.google_dt
SELECT gs.*,
       COALESCE(themes.node_id, '') AS node_id,
       SPLIT_PART(gs.page, '/', 3)  AS page_urlhost,
       title,
       theme_id,
       subtheme_id,
       topic_id,
       subtopic_id,
       subsubtopic_id,
       theme,
       subtheme,
       topic,
       subtopic,
       subsubtopic
FROM   google.googlesearch AS gs
       LEFT JOIN google.google_sites r
              ON gs.site = r.ref_site
       -- fix for misreporting of redirected front page URL in Google search
       LEFT JOIN cmslite.themes AS themes
              ON CASE
                   WHEN page = 'https://www2.gov.bc.ca/' THEN
                   'https://www2.gov.bc.ca/gov/content/home'
                   ELSE page
                 END = themes.hr_url
WHERE
        gs.site NOT IN (
            'sc-domain:gov.bc.ca',
            'sc-domain:engage.gov.bc.ca'
        )
    -- Case where data collected by site and sc-domain overlaps
        OR (
            gs.site = 'sc-domain:gov.bc.ca'
            AND page_urlhost = r.sc_urlhost
            AND gs.DATE :: DATE < r.start_date :: DATE
        )
    -- All other sc-domain data, excluding sites collected directly
        OR (
            gs.site = 'sc-domain:gov.bc.ca'
            AND r.sc_domain = 'f'
            AND page_urlhost NOT IN (
            SELECT
                sc_urlhost
            FROM
                google.google_sites
            WHERE
                sc_urlhost IS NOT NULL
            )
    )
    OR (gs.site = 'sc-domain:engage.gov.bc.ca');

ANALYZE cmslite.google_dt;

COMMIT;
