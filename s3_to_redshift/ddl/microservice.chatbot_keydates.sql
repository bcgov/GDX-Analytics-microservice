CREATE TABLE IF NOT EXISTS microservice.chatbot_keydates
 (
    "date" DATE ENCODE ZSTD,
    "text" VARCHAR(255) ENCODE ZSTD
 )
 DISTSTYLE EVEN;
 ALTER TABLE microservice.chatbot_keydates owner to microservice;
 GRANT SELECT ON microservice.chatbot_keydates TO "looker";
