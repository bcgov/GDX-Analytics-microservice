--DROP TABLE microservice.history;
CREATE TABLE IF NOT EXISTS microservice.looker_history
(
        id INTEGER NOT NULL  ENCODE zstd
        ,user_id INTEGER   ENCODE zstd
        ,title VARCHAR(255)   ENCODE zstd
        ,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
        ,query_id INTEGER   ENCODE zstd
        ,look_id INTEGER   ENCODE zstd
        ,runtime DOUBLE PRECISION   ENCODE zstd
        ,source VARCHAR(255)   ENCODE zstd
        ,node_id INTEGER   ENCODE zstd
        ,status VARCHAR(255)   ENCODE zstd
        ,slug VARCHAR(255)   ENCODE zstd
        ,cache_key VARCHAR(255)   ENCODE zstd
        ,result_source VARCHAR(255)   ENCODE zstd
        ,message VARCHAR(1024)   ENCODE zstd
        ,connection_name VARCHAR(255)   ENCODE zstd
        ,connection_id VARCHAR(255)   ENCODE zstd
        ,dialect VARCHAR(255)   ENCODE zstd
        ,completed_at TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
        ,force_production INTEGER   ENCODE zstd
        ,generate_links INTEGER   ENCODE zstd
        ,path_prefix_id INTEGER   ENCODE zstd
        ,"cache" INTEGER   ENCODE zstd
        ,cache_only INTEGER   ENCODE zstd
        ,sql_query_id INTEGER   ENCODE zstd
        ,render_key VARCHAR(255)   ENCODE zstd
        ,rebuild_pdts INTEGER   ENCODE zstd
        ,server_table_calcs INTEGER   ENCODE zstd
        ,dashboard_id VARCHAR(255)   ENCODE zstd
        ,result_format VARCHAR(255)   ENCODE zstd
        ,apply_formatting INTEGER   ENCODE zstd
        ,dashboard_session VARCHAR(255)   ENCODE zstd
        ,apply_vis INTEGER   ENCODE zstd
        ,models_dir VARCHAR(255)   ENCODE zstd
        ,workspace_id VARCHAR(255)   ENCODE zstd
        ,result_maker_id INTEGER   ENCODE zstd
)
DISTSTYLE AUTO
 SORTKEY (
        created_at
        )
;
ALTER TABLE microservice.looker_history owner to microservice;
GRANT SELECT ON microservice.looker_history to looker;
