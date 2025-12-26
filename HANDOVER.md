# Техническая передача проекта (Handover)

## Текущее состояние: ГОТОВ К PRODUCTION ✅

## Архитектура
```
Google Sheets → CDC/Fast Loader → staging (*_cur, *_hst)
                                        ↓
                          transform_to_public.py
                                        ↓
                                public.* ← CRM Web App
```

## Компоненты

| Файл | Назначение |
|------|------------|
| `run_pipeline.py` | Главный скрипт |
| `cdc_loader.py` | Инкрементальная загрузка (по умолчанию) |
| `fast_loader.py` | Full Refresh |
| `transform_to_public.py` | Трансформация |
| `src/cdc.py` | CDC логика (хеширование) |

## Использование
```bash
# Стандартный запуск (CDC)
python run_pipeline.py

# Full Refresh
python run_pipeline.py --full-refresh
```

## Scheduler
GitHub Actions: `.github/workflows/elt.yml`
- Автозапуск: ежедневно 6:00 UTC
- Ручной запуск через GitHub UI

## Критичные настройки
- **PgBouncer**: `statement_cache_size=0`
- **Порт**: 6543
- **Схема**: `pl_crm_from_gas/supabase/schema.sql`

## Secrets (GitHub)
| Secret | Источник |
|--------|----------|
| `SUPABASE_URL` | Dashboard → API |
| `SUPABASE_KEY` | Dashboard → API |
| `SUPABASE_DB_URL` | Dashboard → Database |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Файл из secrets/ |

## Все задачи выполнены ✅
- [x] Full Refresh + Skip/Log
- [x] CDC (по умолчанию)
- [x] Трансформация staging → public
- [x] GitHub Actions Scheduler
- [x] 15 unit-тестов
