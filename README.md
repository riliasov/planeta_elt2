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

### Только трансформация
Пропускает загрузку из Google Sheets, выполняет только SQL трансформации (staging -> public).
```bash
python src/main.py --transform-only
```