# Техническая передача проекта (Handover)

## Текущее состояние
Реализован полный ELT-пайплайн с CDC (хеширование строк) и трансформацией в структурированные таблицы.

## Архитектура

```
Google Sheets → fast_loader.py → staging (*_cur, *_hst)
                                      ↓
                        transform_to_public.py
                                      ↓
                              public.* таблицы ← CRM Web App
```

## Компоненты

### 1. Загрузка (fast_loader.py)
- Full Refresh с skip + log для битых строк
- Вычисление `__row_hash` для каждой строки (CDC)
- Статистика загрузки в конце

### 2. Трансформация (transform_to_public.py)
- Маппинг колонок: русские → английские
- Конвертация типов: text → date, numeric
- UPSERT по `legacy_id`

### 3. CDC (src/cdc.py)
- Вычисление MD5-хеша строки
- Класс CDCProcessor для определения INSERT/UPDATE/DELETE

### 4. Pipeline (run_pipeline.py)
```bash
# Полный пайплайн
python3 run_pipeline.py

# Только трансформация (данные уже загружены)
python3 run_pipeline.py --transform-only

# Только загрузка (без трансформации)
python3 run_pipeline.py --skip-transform
```

## Критичные настройки
- **PgBouncer**: `statement_cache_size=0` обязателен
- **Порт**: 6543 (транзакционный пулер)
- **Схема**: CRM владеет схемой (`pl_crm_from_gas/supabase/schema.sql`)

## Файлы конфигурации

| Файл | Назначение |
|------|------------|
| `.env` | Подключение к Supabase |
| `sources.yml` | Маппинг Sheets → таблицы |
| `headers.json` | Кеш заголовков (генерируется) |

## Связь с CRM

CRM-приложение (`pl_crm_from_gas`) читает данные из `public.*`:
- `public.clients` — клиенты
- `public.schedule` — расписание
- `public.sales` — продажи

Контракт данных: `pl_crm_from_gas/docs/DATA_CONTRACTS.md`

## Следующие шаги
- [ ] Интеграция CDC в режиме инкрементального обновления
- [ ] Настройка cron/scheduler для автоматического запуска
- [ ] Мониторинг и алерты при сбоях
