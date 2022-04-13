CREATE TABLE IF NOT EXISTS microservice.chatbot_reports
 (
    "dashboard_id" VARCHAR(63) ENCODE ZSTD,
    "conf_score" VARCHAR(63) ENCODE ZSTD,
    "sentiment_score" DECIMAL(3,2) ENCODE ZSTD,
    "sentiment_magnitude" DECIMAL(3,2) ENCODE ZSTD,
    "conversation_length" DECIMAL ENCODE ZSTD,
    "conversation_duration" DECIMAL ENCODE ZSTD,
    "sessionorigin" VARCHAR(255) ENCODE ZSTD,
    "session_id" VARCHAR(255) ENCODE ZSTD,
    "timestamp" VARCHAR(255) ENCODE ZSTD,
    "matched_intent" VARCHAR(255) ENCODE ZSTD,
    "training_result" VARCHAR(255) ENCODE ZSTD,
    "conversation_rating" VARCHAR(255) ENCODE ZSTD,
    "issue" VARCHAR(255) ENCODE ZSTD
 )
 DISTSTYLE EVEN;
 ALTER TABLE microservice.chatbot_reports owner to microservice;
 GRANT SELECT ON microservice.chatbot_reports TO "looker";