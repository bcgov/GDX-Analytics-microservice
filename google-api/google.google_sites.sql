CREATE TABLE google.google_sites (
  ref_site VARCHAR(255) ENCODE ZSTD, 
  sc_domain boolean ENCODE ZSTD, 
  overlap boolean ENCODE ZSTD, 
  start_date date ENCODE AZ64, 
  sc_urlhost VARCHAR(255) ENCODE ZSTD
);
