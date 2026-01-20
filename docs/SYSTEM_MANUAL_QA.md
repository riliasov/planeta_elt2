# Q&A: Ответы на вопросы к SYSTEM_MANUAL

**Дата:** 18.01.2026

---

## 1. Асинхронность: Работает ли для листов в разных файлах Sheets?

**Ответ:** ❌ Нет.

Текущая реализация (`pipeline.py`) обрабатывает листы **последовательно**, даже если они из разных spreadsheets. 

**Причина:** Код итерирует `for spreadsheet_id in config.get('spreadsheets', {}).items()` затем `for sheet_cfg in sdata.get('sheets', [])` — это вложенный цикл без `asyncio.gather`.

**Рекомендация для улучшения:**
```python
# Вместо последовательной обработки:
for sheet_cfg in sheets:
    await process_sheet(sheet_cfg)

# Можно использовать параллельную (с ограничением):
import asyncio
sem = asyncio.Semaphore(5)  # Ограничение 5 одновременно
await asyncio.gather(*[process_with_sem(s, sem) for s in all_sheets])
```

---

## 2. Контракты: Насколько жёсткие? Примеры

**Ответ:** Контракты — это JSON Schema файлы в `src/contracts/`. 

**Пример (`clients.json`):**
```json
{
  "columns": [
    {"name": "ФИО", "type": "string", "required": true},
    {"name": "Телефон", "type": "string", "required": true},
    {"name": "Дата рождения", "type": "date", "format": "DD.MM.YYYY"},
    {"name": "Баланс", "type": "money"}
  ]
}
```

**Уровни жёсткости:**
| Поле | Правило | Последствие |
|---|---|---|
| `required: true` | Пустое значение = ошибка | Строка пропадает из загрузки |
| `type: "date"` | Формат должен соответствовать паттерну | Ошибка валидации |
| `type: "money"` | Очистка от символов, парсинг числа | При неуспехе — ошибка |

---

## 3. Поиск строки: Как настроен? Улучшения?

**Ответ:** Поиск строки происходит в `extractor.py`:
1.  Читается весь `range` (`A1:ZZ`).
2.  Первая строка считается заголовком.
3.  Остальные — данные.

**Ограничение:** Нет фильтрации пустых строк до загрузки.

**Рекомендация:**
```python
# Добавить пропуск полностью пустых строк:
aligned_rows = [r for r in aligned_rows if any(cell is not None and str(cell).strip() for cell in r)]
```

---

## 4. Логи валидации: Нужен явный вывод для пользователя

**Ответ:** ✅ Согласен.

**Текущее состояние:** Логи пишутся в таблицу `validation_logs` и в stdout (Python logger).

**Рекомендация:**
*   Добавить CLI-флаг `--show-validation-report` для вывода топ-N ошибок в терминал.
*   Или формировать `validation_report.md` после каждого run.

---

## 5. Правило ошибок: 20 на любую таблицу? А если 100k строк?

**Ответ:** ✅ Справедливое замечание. Абсолютный порог 20 не масштабируется.

**Рекомендация:** Перейти на **процентный порог**:
```python
MAX_ERROR_RATE = 0.02  # 2%
if validation_errors / total_rows > MAX_ERROR_RATE:
    raise ValueError("...")
```

---

## 6. Квоты API: >100 таблиц?

**Ответ:** Уточнение от пользователя: **не более 20 таблиц** в проекте.

При 20 таблицах риск 429 минимален (штатные 60 запросов/мин).

---

## 7. Лимит строк: 500k?

**Ответ:** Уточнение от пользователя: **не более 100k строк** на таблицу.

При 100k строк и ~20 колонках потребуется ~80-150 MB RAM — приемлемо.

---

## 8. Защита от конфликтов: Реализована ли?

**Ответ:** ⚠️ **Не реализована полностью.**

**Что сделано:**
*   GAS скрипт (`pk_cdc_master.gs`) валидирует UUID, но **не блокирует редактирование**.
*   ETL пайплайн не проверяет `source_system`.

**Что нужно для полной реализации (Backlog):**
1.  В GAS: добавить `onEdit` проверку `if source_system === 'webapp' -> Block`.
2.  В ETL: добавить логику "Ticket System" при несовпадении владельца.

---

## 9. Добавление SQL-скриптов: Подробнее

**Ответ:** Пошаговая инструкция:

1.  **Создать SQL-файл:**
    ```sql
    -- src/db/sql/transform_inventory.sql
    INSERT INTO public.inventory (id, name, qty)
    SELECT record_id, product_name, COALESCE(quantity, 0)
    FROM staging.inventory_cur
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      qty = EXCLUDED.qty,
      updated_at = NOW();
    ```

2.  **Зарегистрировать в Transformer:**
    ```python
    # src/etl/transformer.py
    files_to_run = [
        'transform_clients.sql',
        'transform_schedule.sql',
        'transform_sales.sql',
        'transform_inventory.sql',  # ← Добавить
    ]
    ```

3.  **Запустить ETL:**
    ```bash
    python -m src.main --skip-load
    ```

---

## 10. Новые источники: Bitrix, Telegram?

**Ответ:** ✅ Да, через API.

Архитектура позволяет создать:
*   `BitrixExtractor` — через REST API Bitrix24.
*   `TelegramExtractor` — через Telethon/Pyrogram + Supabase.

**Условие:** Новый экстрактор должен возвращать `(col_names: List[str], rows: List[List[Any]])` — стандартный интерфейс.

---
*Документ подготовлен по комментариям пользователя.*
