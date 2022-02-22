CREATE TABLE IF NOT EXISTS servicebc.dashboard_user 
(
  "id" INT NOT NULL ENCODE ZSTD,
  "is_admin" INT ENCODE ZSTD,
  "first_name" VARCHAR(255) ENCODE ZSTD,
  "last_name" VARCHAR(255) ENCODE ZSTD,
  "models" VARCHAR(255) ENCODE ZSTD,
  "models_dir" VARCHAR(255) ENCODE ZSTD,
  "dev_mode" INT ENCODE ZSTD,
  "chat_popover" INT ENCODE ZSTD,
  "browser_count" INT ENCODE ZSTD,
  "is_looker_employee" INT NOT NULL ENCODE ZSTD,
  "dev_mode_user_id" INT ENCODE ZSTD,
  "is_disabled" INT ENCODE ZSTD,
  "email" VARCHAR(255) ENCODE ZSTD,
  "marketing_email_updates" INT ENCODE ZSTD,
  "release_email_updates" INT ENCODE ZSTD,
  "outgoing_access_token_id" INT ENCODE ZSTD,
  "eula_accepted" INT ENCODE ZSTD,
  "eula_accepted_time" TIMESTAMP ENCODE ZSTD,
  "eula_accepted_version" VARCHAR(255) ENCODE ZSTD,
  "home_space_id" VARCHAR(255) ENCODE ZSTD,
  "chat_disabled" INT ENCODE ZSTD,
  "dev_branch_name" VARCHAR(255) ENCODE ZSTD,
  "timezone" VARCHAR(255) ENCODE ZSTD,
  "editor_keybinding_mode" VARCHAR(255) ENCODE ZSTD,
  "version_set_id" INT ENCODE ZSTD,
  "ui_state" VARCHAR(255) ENCODE ZSTD,
  "external_avatar_url" VARCHAR(255) ENCODE ZSTD,
  "sticky_workspace_id" VARCHAR(255) ENCODE ZSTD,
  "locale" VARCHAR(255) ENCODE ZSTD,
  "created_at" TIMESTAMP ENCODE ZSTD,
  "models_dir_validated" INT ENCODE ZSTD,
  "prev_permissions" VARCHAR(2048) ENCODE ZSTD,
  "tokens_invalid_before" TIMESTAMP ENCODE ZSTD,
  "auto_provisioned_api_user" INT ENCODE ZSTD,
  "requires_email_verification" INT ENCODE ZSTD
);

GRANT SELECT ON servicebc.user TO "looker";