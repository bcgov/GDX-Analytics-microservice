CREATE TABLE IF NOT EXISTS microservice.looker_dashboard 
(
  "id" INT ENCODE ZSTD NOT NULL,
  "user_id" INT ENCODE ZSTD,
  "title" VARCHAR(255) ENCODE ZSTD,
  "description" VARCHAR(255) ENCODE ZSTD,
  "hidden" INT ENCODE ZSTD,
  "created_at" TIMESTAMP ENCODE ZSTD,
  "space_id" INT ENCODE ZSTD,
  "deleted_at" TIMESTAMP ENCODE ZSTD,
  "filter_lookml" VARCHAR(255) ENCODE ZSTD,
  "refresh_interval" VARCHAR(255) ENCODE ZSTD,
  "load_configuration" VARCHAR(255) ENCODE ZSTD,
  "background_color" VARCHAR(255) ENCODE ZSTD,
  "show_title" INT ENCODE ZSTD,
  "title_color" VARCHAR(255) ENCODE ZSTD,
  "show_filters_bar" INT ENCODE ZSTD,
  "tile_background_color" VARCHAR(255) ENCODE ZSTD,
  "tile_text_color" VARCHAR(255) ENCODE ZSTD,
  "tile_separator_color" VARCHAR(255) ENCODE ZSTD,
  "tile_border_radius" INT ENCODE ZSTD,
  "show_tile_shadow" INT ENCODE ZSTD,
  "content_metadata_id" INT ENCODE ZSTD,
  "text_tile_text_color" VARCHAR(255) ENCODE ZSTD,
  "deleter_id" INT ENCODE ZSTD,
  "query_timezone" VARCHAR(64) ENCODE ZSTD,
  "lookml_link_id" VARCHAR(255) ENCODE ZSTD,
  "preferred_viewer" VARCHAR(255) ENCODE ZSTD,
  "appearance" VARCHAR(255) ENCODE ZSTD,
  "crossfilter_enabled" INT ENCODE ZSTD,
  "indexed_at" TIMESTAMP ENCODE ZSTD,
  "alert_sync_with_dashboard_filter_enabled" INT ENCODE ZSTD,
  "filters_bar_collapsed" INT ENCODE ZSTD,
  "updated_at" TIMESTAMP ENCODE ZSTD,
  "last_updater_id" INT ENCODE ZSTD
);

GRANT SELECT ON microservice.looker_dashboard TO "looker";