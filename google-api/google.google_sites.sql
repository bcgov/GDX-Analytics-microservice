CREATE TABLE google.google_sites (
  ref_site VARCHAR(255) ENCODE ZSTD, 
  sc_domain BOOLEAN ENCODE ZSTD, 
  overlap BOOLEAN ENCODE ZSTD, 
  start_date DATE ENCODE AZ64, 
  sc_urlhost VARCHAR(255) ENCODE ZSTD
);
ALTER TABLE google.google_sites OWNER TO microservice;
GRANT SELECT ON TABLE google.google_sites TO LOOKER; 
