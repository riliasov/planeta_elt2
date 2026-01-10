# Scheduler: Автоматический запуск ELT

## GitHub Actions

Пайплайн запускается:
- **Автоматически**: каждый день в 6:00 UTC (10:00 MSK)
- **Вручную**: через GitHub → Actions → Run workflow

## Настройка Secrets

В GitHub репозитории → Settings → Secrets and variables → Actions:

| Secret | Описание | Откуда взять |
|--------|----------|--------------|
| `SUPABASE_URL` | URL проекта | Dashboard → Settings → API |
| `SUPABASE_KEY` | Anon Key | Dashboard → Settings → API |
| `SUPABASE_DB_URL` | Connection string | Dashboard → Settings → Database |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Весь JSON файл | secrets/google-service-account.json |

### Пример SUPABASE_DB_URL
```
postgresql://postgres.xxx:PASSWORD@aws-1-eu-west-1.pooler.supabase.com:6543/postgres
```

### Пример GOOGLE_SERVICE_ACCOUNT_JSON
Скопировать всё содержимое файла `secrets/google-service-account.json`:
```json
{
  "type": "service_account",
  "project_id": "...",
  ...
}
```

## Ручной запуск

1. GitHub → Actions → ELT Pipeline
2. Run workflow
3. Выбрать опции:
   - `skip_load` — только трансформация
   - `transform_only` — пропустить загрузку

## Изменение расписания

В файле `.github/workflows/elt.yml`:
```yaml
schedule:
  - cron: '0 6 * * *'  # Каждый день в 6:00 UTC
```

Примеры cron:
- `0 */6 * * *` — каждые 6 часов
- `0 6 * * 1-5` — пн-пт в 6:00
- `0 6,18 * * *` — дважды в день

## Локальный запуск (без GitHub)

```bash
cd /Users/rambook/Developer/planeta/pl-etl-core
source .venv/bin/activate
python run_pipeline.py
```
