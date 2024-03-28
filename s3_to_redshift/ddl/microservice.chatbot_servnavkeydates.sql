CREATE TABLE IF NOT EXISTS microservice.chatbot_servnavkeydates
 (
    "date" DATE ENCODE ZSTD,
    "text" VARCHAR(255) ENCODE ZSTD
 )
 DISTSTYLE EVEN;
 ALTER TABLE microservice.chatbot_servnavkeydates owner to microservice;
 GRANT SELECT ON microservice.chatbot_servnavkeydates TO "looker";
