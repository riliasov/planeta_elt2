# Project Backlog

## Pending
- [ ] **WebApp Migration**: Migrate `public` schema tables (`auth_*`, `products`, `employees`, `schedule`, `notification_*`) to `webapp` schema using the provided agent prompt.
- [ ] **RLS Policies**: Configure Row-Level Security for `service_role` and `webapp` users after migration.
- [ ] **Harmonization**: Complete the move of any remaining Google Sheets logic to `stg_gsheets` if new sheets are added.

## Future
- [ ] **Data Marts**: Move `analytics` layer to Materialized Views in `marts` schema if performance becomes an issue.
- [ ] **Monitoring**: Add alerting on `ops.validation_logs` thresholds.
