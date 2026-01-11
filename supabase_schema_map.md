# Supabase Database Map (Schema: public)

## Table of Contents
- [auth_attempts](#table-auth-attempts)
- [auth_events](#table-auth-events)
- [auth_requests](#table-auth-requests)
- [auth_sessions](#table-auth-sessions)
- [auth_whitelist](#table-auth-whitelist)
- [clients](#table-clients)
- [elt_runs](#table-elt-runs)
- [elt_table_stats](#table-elt-table-stats)
- [employees](#table-employees)
- [expenses](#table-expenses)
- [notification_queue](#table-notification-queue)
- [notification_rules](#table-notification-rules)
- [products](#table-products)
- [sales](#table-sales)
- [schedule](#table-schedule)
- [telegram_chats](#table-telegram-chats)
- [telegram_health](#table-telegram-health)
- [telegram_messages](#table-telegram-messages)
- [telegram_sync_queue](#table-telegram-sync-queue)
- [telegram_users](#table-telegram-users)
- [validation_logs](#table-validation-logs)

## Table: auth_attempts
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| phone | text | NO | None |
| otp_hash | text | NO | None |
| magic_token_hash | text | NO | None |
| status | text | YES | 'pending'::text |
| attempts_count | integer | YES | 0 |
| created_at | timestamp with time zone | YES | now() |
| expires_at | timestamp with time zone | NO | None |

## Table: auth_events
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| action | text | NO | None |
| phone | text | NO | None |
| success | boolean | NO | false |
| user_id | uuid | YES | None |
| ip_address | text | YES | None |
| user_agent | text | YES | None |
| details | text | YES | None |
| created_at | timestamp with time zone | YES | now() |

## Table: auth_requests
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| phone | text | NO | None |
| chat_id | bigint | NO | None |
| name | text | YES | None |
| status | text | YES | 'pending'::text |
| approved_by_chat_id | bigint | YES | None |
| created_at | timestamp with time zone | YES | now() |
| expires_at | timestamp with time zone | YES | (now() + '00:05:00'::interval) |

## Table: auth_sessions
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| user_id | uuid | NO | None |
| token_hash | text | NO | None |
| device_hash | text | YES | None |
| revoked | boolean | YES | false |
| revoked_at | timestamp with time zone | YES | None |
| expires_at | timestamp with time zone | NO | None |
| created_at | timestamp with time zone | YES | now() |

## Table: auth_whitelist
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| phone | text | NO | None |
| chat_id | bigint | YES | None |
| name | text | NO | None |
| role | text | YES | 'user'::text |
| is_active | boolean | YES | true |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |

## Table: clients
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| legacy_id | text | YES | None |
| name | text | NO | None |
| phone | text | NO | None |
| child_name | text | YES | None |
| child_dob | date | YES | None |
| age | text | YES | None |
| spent | numeric | NO | 0 |
| balance | numeric | NO | 0 |
| debt | numeric | NO | 0 |
| status | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |
| row_hash | text | YES | None |
| source | text | YES | None |
| deleted_at | timestamp with time zone | YES | None |
| is_deleted | boolean | YES | false |

## Table: elt_runs
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| run_id | uuid | NO | None |
| started_at | timestamp with time zone | NO | now() |
| finished_at | timestamp with time zone | YES | None |
| status | text | NO | 'running'::text |
| mode | text | NO | 'cdc'::text |
| tables_processed | integer | YES | 0 |
| total_rows_synced | integer | YES | 0 |
| validation_errors | integer | YES | 0 |
| duration_seconds | numeric | YES | None |
| error_message | text | YES | None |

## Table: elt_table_stats
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | bigint | NO | nextval('elt_table_stats_id_seq'::regclass) |
| run_id | uuid | NO | None |
| table_name | text | NO | None |
| rows_extracted | integer | YES | 0 |
| rows_inserted | integer | YES | 0 |
| rows_updated | integer | YES | 0 |
| rows_deleted | integer | YES | 0 |
| rows_unchanged | integer | YES | 0 |
| validation_errors | integer | YES | 0 |
| duration_ms | integer | YES | None |
| created_at | timestamp with time zone | YES | now() |

## Table: employees
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| legacy_id | text | YES | None |
| name | text | NO | None |
| type | text | YES | None |
| email | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |

## Table: expenses
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | bigint | NO | None |
| date | date | YES | None |
| category | text | YES | None |
| amount | numeric | YES | None |
| comment | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |
| row_hash | text | YES | None |
| source | text | YES | None |
| deleted_at | timestamp with time zone | YES | None |
| is_deleted | boolean | YES | false |

## Table: notification_queue
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| rule_id | uuid | YES | None |
| status | text | YES | 'pending'::text |
| target | text | NO | None |
| message | text | NO | None |
| error_log | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| sent_at | timestamp with time zone | YES | None |

## Table: notification_rules
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| name | text | NO | None |
| trigger_type | text | NO | None |
| target_type | text | YES | 'telegram_chat'::text |
| target_id | text | NO | None |
| is_active | boolean | YES | true |
| message_template | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |

## Table: products
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| legacy_id | text | YES | None |
| name | text | NO | None |
| type | text | YES | None |
| category | text | YES | None |
| quantity | integer | NO | 0 |
| price | numeric | NO | 0 |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |

## Table: sales
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| legacy_id | text | YES | None |
| date | timestamp with time zone | NO | now() |
| client_id | uuid | NO | None |
| product_name | text | NO | None |
| type | text | YES | None |
| category | text | YES | None |
| quantity | integer | NO | 1 |
| full_price | numeric | NO | None |
| discount | numeric | NO | 0 |
| final_price | numeric | NO | None |
| cash | numeric | NO | 0 |
| transfer | numeric | NO | 0 |
| terminal | numeric | NO | 0 |
| debt | numeric | NO | 0 |
| admin_id | uuid | YES | None |
| trainer_id | uuid | YES | None |
| comment | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |
| row_hash | text | YES | None |
| source | text | YES | None |
| deleted_at | timestamp with time zone | YES | None |
| is_deleted | boolean | YES | false |

## Table: schedule
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| legacy_id | text | YES | None |
| date | date | NO | None |
| start_time | time without time zone | NO | None |
| end_time | time without time zone | NO | None |
| employee_id | uuid | YES | None |
| client_id | uuid | YES | None |
| status | text | NO | 'Свободно'::text |
| type | text | YES | None |
| category | text | YES | None |
| remaining_lessons | integer | NO | 0 |
| total_visited | integer | NO | 0 |
| replace_info | text | YES | None |
| comment | text | YES | None |
| whatsapp_reminder | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |
| row_hash | text | YES | None |
| source | text | YES | None |
| deleted_at | timestamp with time zone | YES | None |
| is_deleted | boolean | YES | false |

## Table: telegram_chats
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | bigint | NO | None |
| title | text | YES | None |
| type | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |

## Table: telegram_health
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| service_id | text | NO | None |
| last_heartbeat_at | timestamp with time zone | YES | now() |
| last_successful_sync_at | timestamp with time zone | YES | None |
| status | text | YES | 'idle'::text |
| meta | jsonb | YES | '{}'::jsonb |

## Table: telegram_messages
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | uuid | NO | gen_random_uuid() |
| telegram_id | bigint | NO | None |
| chat_id | bigint | YES | None |
| user_id | bigint | YES | None |
| text | text | YES | None |
| timestamp | timestamp with time zone | NO | None |
| reply_to_message_id | bigint | YES | None |
| is_outgoing | boolean | YES | false |
| has_keyword | boolean | YES | false |
| is_reaction | boolean | YES | false |
| is_deleted | boolean | YES | false |
| deleted_at | timestamp with time zone | YES | None |
| is_edited | boolean | YES | false |
| edit_version | integer | YES | 0 |
| original_msg_id | bigint | YES | None |
| created_at | timestamp with time zone | YES | now() |
| read_at | timestamp with time zone | YES | None |
| is_test | boolean | YES | false |

## Table: telegram_sync_queue
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | bigint | NO | None |
| type | text | NO | None |
| status | text | YES | 'pending'::text |
| params | jsonb | YES | '{}'::jsonb |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |
| processed_at | timestamp with time zone | YES | None |
| error_message | text | YES | None |

## Table: telegram_users
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | bigint | NO | None |
| username | text | YES | None |
| first_name | text | YES | None |
| last_name | text | YES | None |
| contact_name | text | YES | None |
| phone | text | YES | None |
| is_self | boolean | YES | false |
| is_lead | boolean | YES | false |
| lead_status | text | YES | None |
| lead_date | timestamp with time zone | YES | None |
| last_message_at | timestamp with time zone | YES | None |
| first_message_direction | text | YES | None |
| created_at | timestamp with time zone | YES | now() |
| updated_at | timestamp with time zone | YES | now() |

## Table: validation_logs
| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| id | bigint | NO | nextval('validation_logs_id_seq'::regclass) |
| run_id | uuid | NO | None |
| table_name | text | NO | None |
| row_index | integer | NO | None |
| column_name | text | YES | None |
| invalid_value | text | YES | None |
| error_type | text | NO | None |
| message | text | NO | None |
| created_at | timestamp with time zone | YES | now() |

