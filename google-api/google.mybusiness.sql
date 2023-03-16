DROP TABLE IF EXISTS google.mybusiness;
CREATE TABLE IF NOT EXISTS google.mybusiness (
    date   DATE,
    client   VARCHAR(255)    ENCODE ZSTD,
    location   VARCHAR(255)    ENCODE ZSTD,
    location_id   VARCHAR(255)    ENCODE ZSTD,
    business_impressions_desktop_maps   INTEGER,
    business_impressions_desktop_search   INTEGER,
    business_impressions_mobile_maps   INTEGER,
    business_impressions_mobile_search   INTEGER,
    business_conversations   INTEGER,
    business_direction_request   INTEGER,
    call_clicks   INTEGER,
    website_clicks   INTEGER,
    business_bookings   INTEGER
);
ALTER TABLE google.mybusiness OWNER TO microservice;
GRANT SELECT ON TABLE google.mybusiness TO LOOKER;
