# Planeta ELT Pipeline v2

ETL пайплайн для синхронизации данных из Google Sheets в Supabase (PostgreSQL).
Поддерживает инкрементальную загрузку (CDC) и полный рефреш.

## Особенности (v2)
- **File Logging**: Логи сохраняются в `logs/elt.log` с ротацией.
- **Strict Validation**: Валидация данных с использованием Pydantic моделей (поддержка транслитерации и переименования).
- **Admin Dashboard**: Веб-интерфейс (Streamlit) для мониторинга статуса и ошибок.
- **CI/CD**: Автоматический запуск тестов через GitHub Actions.
- **PostgreSQL Schema**: Автоматическая миграция схемы и поддержка soft delete.

## Структура проекта

* `src/` - Исходный код
    * `etl/` - Основная логика ETL (Extractor, Loader, Transformer)
    * `db/` - Работа с базой данных
    * `config/` - Настройки
    * `utils/` - Утилиты
* `legacy/` - Старые скрипты (архив)
* `sources.yml` - Конфигурация источников данных
* `requirements.txt` - Зависимости

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Настройте `.env` (см. HANDOVER.md).

## Использование

Основной скрипт запуска: `src/main.py`.

### Инкрементальная загрузка (CDC)
Стандартный режим. Загружает только измененные данные.
```bash
python src/main.py
```

### Полная перезагрузка (Full Refresh)
Очищает целевые таблицы и загружает всё заново.
```bash
python src/main.py --full-refresh
```

### Обновление схемы (Schema Deploy)
Пересоздает staging таблицы (`*_cur`) на основе текущих заголовков Google Sheets.
**Внимание**: удаляет данные из staging таблиц! Рекомендуется запускать вместе с `--full-refresh`.
```bash
python src/main.py --deploy-schema --full-refresh
```

## Новые инструменты (v2.1 - Modular Architecture)

Добавлены специализированные инструменты для повышения надежности и скорости:

- **`scripts/migrate_hst.py`**: Безопасная первичная миграция истории. Использует **Atomic Swap** (через временную таблицу `_new`), сравнивает объемы данных и проверяет дубликаты PK/Hash перед применением.
- **`scripts/etl_diagnose.py`**: Диагностика доступа к Google Sheets, оценка объема данных и автоматический поиск строки заголовков CDC.
- **`tests/test_schema_integrity.py`**: Автоматический тест на соответствие JSON-контрактов реальной схеме БД (защита от "расползания" структуры).

### Пример быстрой миграции:
```bash
python scripts/migrate_hst.py --sheets sales_hst,clients_hst --confirm
```

---

### Только трансформация
Пропускает загрузку из Google Sheets, выполняет только SQL трансформации (stg_gsheets -> core).
```bash
python src/main.py --transform-only
```

### Только трансформация
Пропускает загрузку из Google Sheets, выполняет только SQL трансформации (stg_gsheets -> core).
```bash
python src/main.py --transform-only
```