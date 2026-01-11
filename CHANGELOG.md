# Changelog

## [Unreleased] - 2026-01-11

### Added
- **4-Layer Architecture**: Implemented `stg_gsheets` (Sheets), `telegram` (TG), `core` (Logic), `ops` (Logs) schemas.
- **Unified View**: Created `core.unified_customers` view merging CRM (Sheets) and Telegram data.
- **Migration Scripts**: Added scripts for schema creation, data migration, and cleanup.
- **Prompt**: Added `webapp_migration_prompt.md` for external agent to handle WebApp DB migration.

### Changed
- **Database Schema**: Moved `public.clients`, `public.sales`, etc. to `stg_gsheets`.
- **Configuration**: Updated `sources.yml` to target `stg_gsheets.*` tables.
- **Pipeline**: Updated `ELTPipeline` and `TableProcessor` to log run stats and errors to `ops` schema (`ops.elt_runs`, `ops.validation_logs`).
- **Security**: Hardened strict regex validation for identifiers in `loader.py` (allowing points for schema support).
- **Cleanup**: Removed empty legacy tables (`raw.clients_hst`, `raw.sales_cur` etc) from `raw` schema.

### Fixed
- **Naming**: Corrected Telegram User ID reference (used `id` instead of `user_id`) in unified view.
