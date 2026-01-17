# План Реализации: ТОП-3 Критичных Задач

> **Дата**: 2026-01-17  
> **Контекст**: Приоритетные задачи из BACKLOG.md, отобранные по критериям: критичность для production, влияние на качество данных, complexity vs impact.

---

## 🎯 Выбранные Задачи (Топ-3 High Priority)

1. **Telegram Alerts** — Критично для observability в production
2. **Validation Fix (expenses)** — Предотвращение потери данных
3. **Business Logic (v_client_balances)** — Корректность бизнес-метрик

---

## 1. Telegram Alerts: Оповещение при критической остановке пайплайна

### Обоснование выбора
**Критичность**: ⚠️ БЛОКЕР для production  
**Impact**: При падении пайплайна в production **никто не узнает**, пока не обнаружат устаревшие данные (может занять дни).  
**Effort**: 🟢 LOW (1-2 часа работы)

### Пошаговый План

#### Шаг 1: Установить зависимость
```bash
echo "requests==2.31.0" >> requirements.txt
pip install -r requirements.txt
```

#### Шаг 2: Создать модуль уведомлений
**Файл**: `src/utils/telegram_notifier.py`

```python
"""Telegram уведомления о критических ошибках."""
import os
import requests
import logging
from typing import Optional

log = logging.getLogger('telegram_notifier')

class TelegramNotifier:
    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_ALERT_CHAT_ID')
        self.enabled = bool(self.bot_token and self.chat_id)
        
        if not self.enabled:
            log.warning("Telegram уведомления отключены (отсутствуют TELEGRAM_BOT_TOKEN или TELEGRAM_ALERT_CHAT_ID)")
    
    def send_alert(self, message: str, parse_mode: str = 'HTML') -> bool:
        """Отправка критического алерта в Telegram."""
        if not self.enabled:
            log.warning(f"Alerta не отправлен (уведомления отключены): {message}")
            return False
        
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            'chat_id': self.chat_id,
            'text': message,
            'parse_mode': parse_mode
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            log.info(f"Telegram alerta отправлен: {message[:50]}...")
            return True
        except Exception as e:
            log.error(f"Ошибка отправки Telegram: {e}")
            return False

# Singleton
notifier = TelegramNotifier()
```

#### Шаг 3: Обновить `.env`
Добавить переменные (пользователь должен создать бота через @BotFather):
```env
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_ALERT_CHAT_ID=-1001234567890
```

#### Шаг 4: Интегрировать в `src/main.py`
Обернуть весь pipeline:

```python
from src.utils.telegram_notifier import notifier

def main():
    try:
        # ... существующий код ...
        asyncio.run(run_pipeline())
    except Exception as e:
        # Критический алерт
        notifier.send_alert(
            f"🚨 <b>ETL PIPELINE FAILED</b>\n\n"
            f"<b>Error:</b> {type(e).__name__}\n"
            f"<code>{str(e)[:500]}</code>\n\n"
            f"<i>Time:</i> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        raise
```

#### Шаг 5: Тестирование
```bash
# Установить env vars
export TELEGRAM_BOT_TOKEN="..."
export TELEGRAM_ALERT_CHAT_ID="..."

# Запустить с намеренной ошибкой
python src/main.py --invalid-flag
```

**Ожидаемый результат**: Сообщение в Telegram канале.

---

## 2. Validation Fix: Создание контрактов для `expenses`

### Обоснование выбора
**Критичность**: ⚠️ ПОТЕРЯ ДАННЫХ  
**Impact**: Без контрактов для `expenses_hst`/`expenses_cur` валидатор либо пропускает все строки, либо отбрасывает их как некорректные.  
**Effort**: 🟡 MEDIUM (2-3 часа: исследование структуры + создание контракта)

### Пошаговый План

#### Шаг 1: Извлечь актуальные заголовки из Sheets
```bash
python scripts/inspect_sheets.py
```

Проверить файл `headers.json`:
```bash
cat headers.json | jq '.expenses_hst'
cat headers.json | jq '.expenses_cur'
```

**Ожидаемый вывод** (пример):
```json
["data", "kategoriya", "summa", "kommentariy", "prikhod_raskhod"]
```

#### Шаг 2: Создать контракт
**Путь**: Нужно определить где хранятся контракты. Проверить:
```bash
find src -name "*.yml" -o -name "*.yaml" | grep -i contract
```

Если контракты в отдельной папке (например, `contracts/`), создать:

**Файл**: `contracts/expenses.yml`

```yaml
entity: expenses
description: "Контракт для расходов (expenses_hst / expenses_cur)"

fields:
  - name: data
    type: string
    required: true
    description: "Дата расхода"
  
  - name: kategoriya
    type: string
    required: false
    description: "Категория (Аренда, Зарплаты, Закупки)"
  
  - name: summa
    type: string  # будет парситься в numeric
    required: true
    description: "Сумма расхода"
  
  - name: kommentariy
    type: string
    required: false
    description: "Комментарий"
  
  - name: prikhod_raskhod
    type: string
    required: false
    description: "Тип операции (Расход/Приход)"
```

#### Шаг 3: Обновить маппинг в `sources.yml`
Найти секции `expenses_hst` и `expenses_cur` (строки 88 и 195):

```yaml
- id: historical_expenses
  gid: "1234567890"
  description: "Расходы_hst"
  range: "A1:ZZ"
  target_table: stg_gsheets.expenses_hst
  mode: upsert
  contract: expenses  # <-- ДОБАВИТЬ
```

#### Шаг 4: Запустить валидацию в dry-run
```bash
python src/main.py --dry-run --skip-transform
```

Проверить логи:
```bash
tail -f logs/etl_*.log | grep -i "expenses"
```

**Ожидаемое**: "✅ expenses_hst validated: X rows, 0 errors"

#### Шаг 5: Проверить `validation_logs`
```bash
python -c "
import asyncio
from src.db.connection import DBConnection

async def check():
    rows = await DBConnection.fetch('''
        SELECT table_name, column_name, error_type, COUNT(*) 
        FROM ops.validation_logs 
        WHERE table_name LIKE '%expenses%'
        GROUP BY 1, 2, 3
        ORDER BY count DESC
        LIMIT 10
    ''')
    for r in rows:
        print(dict(r))
    await DBConnection.close()

asyncio.run(check())
"
```

**Если есть ошибки** — скорректировать контракт.

---

## 3. Business Logic: Переработка `v_client_balances`

### Обоснование выбора
**Критичность**: ⚠️ БИЗНЕС-МЕТРИКА  
**Impact**: Неправильный расчет остатков занятий → клиенты записываются на тренировки без оплаты или наоборот — не могут записаться при наличии баланса.  
**Effort**: 🟡 MEDIUM (3-4 часа: создание lookup таблицы + переписывание view)

### Пошаговый План

#### Шаг 1: Анализ текущих продуктов
Выгрузить уникальные названия продуктов из `core.sales`:

```sql
SELECT DISTINCT product_name, quantity, COUNT(*) as occurrences
FROM core.sales
WHERE is_deleted = false
GROUP BY 1, 2
ORDER BY occurrences DESC
LIMIT 50;
```

**Цель**: Понять, есть ли паттерн "Абонемент X занятий" или продукты разнородные.

#### Шаг 2: Создать миграцию для lookup таблицы

**Файл**: `alembic/versions/XXXX_products_lookup.py`

```sql
-- Создание справочника продуктов
CREATE TABLE IF NOT EXISTS lookups.products (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    units_per_item INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Наполнение начальными данными (на основе анализа)
INSERT INTO lookups.products (name, units_per_item) VALUES
('Абонемент 4 занятия', 4),
('Абонемент 8 занятий', 8),
('Абонемент 12 занятий', 12),
('Абонемент 16 занятий', 16),
('Разовое занятие', 1),
('Пробное занятие', 1)
ON CONFLICT (name) DO NOTHING;
```

Применить:
```bash
alembic upgrade head
```

#### Шаг 3: Переписать view `v_client_balances`

**Файл**: `src/db/sql/view_client_balances.sql`

```sql
-- НОВАЯ ВЕРСИЯ (использует quantity * units_per_item)
CREATE OR REPLACE VIEW analytics.v_client_balances AS
WITH client_sales AS (
    SELECT 
        s.client_id,
        SUM(s.quantity * COALESCE(p.units_per_item, 1)) as units_bought,
        SUM(s.final_price) as total_spent
    FROM core.sales s
    LEFT JOIN lookups.products p ON s.product_name = p.name
    WHERE s.is_deleted = false
    GROUP BY 1
),
client_trainings AS (
    SELECT 
        client_id,
        COUNT(*) as units_used
    FROM core.schedule
    WHERE is_deleted = false
      AND status IN ('Посетили', 'Пропуск')
    GROUP BY 1
)
SELECT 
    c.name as "Клиент",
    c.phone as "Телефон",
    COALESCE(s.units_bought, 0) as "Куплено",
    COALESCE(t.units_used, 0) as "Использовано",
    COALESCE(s.units_bought, 0) - COALESCE(t.units_used, 0) as "Остаток",
    COALESCE(s.total_spent, 0) as "Оплачено",
    c.status as "Статус",
    NOW() as "Дата обновления"
FROM core.clients c
LEFT JOIN client_sales s ON c.id = s.client_id
LEFT JOIN client_trainings t ON c.id = t.client_id
WHERE c.is_deleted = false
  AND (s.units_bought > 0 OR t.units_used > 0)
ORDER BY "Остаток" ASC, c.name ASC;
```

#### Шаг 4: Деплой изменений
```bash
# Запустить transformer (это применит новый view)
python src/main.py --skip-extract --skip-load --skip-export
```

#### Шаг 5: Верификация
Сравнить старые и новые значения:

```sql
-- Запросить метрику из dashboard или напрямую
SELECT "Клиент", "Куплено", "Использовано", "Остаток"
FROM analytics.v_client_balances
WHERE "Остаток" < 0 OR "Остаток" > 100
ORDER BY "Остаток" DESC
LIMIT 10;
```

**Ожидание**: Нет "подозрительных" значений (остаток 800 занятий или -50).

#### Шаг 6: Мониторинг в production
Добавить в dashboard

 страницу "Аномальные балансы":
```python
# В dashboard.py
anomalies = db.fetch("""
    SELECT * FROM analytics.v_client_balances
    WHERE "Остаток" < -5 OR "Остаток" > 50
""")
if anomalies:
    st.warning(f"⚠️ Обнаружено {len(anomalies)} клиентов с аномальным балансом")
    st.dataframe(anomalies)
```

---

## 📋 Чеклист Выполнения

### Telegram Alerts
- [ ] Добавить `requests` в `requirements.txt`
- [ ] Создать `src/utils/telegram_notifier.py`
- [ ] Обновить `.env` с токенами
- [ ] Интегрировать в `src/main.py`
- [ ] Протестировать отправку тестового алерта
- [ ] Задокументировать в README.md инструкции по настройке бота

### Validation Fix (expenses)
- [ ] Запустить `python scripts/inspect_sheets.py`
- [ ] Проверить структуру заголовков `expenses_hst` и `expenses_cur`
- [ ] Создать `contracts/expenses.yml` (или аналог)
- [ ] Обновить `sources.yml` с `contract: expenses`
- [ ] Запустить `--dry-run` и проверить validation_logs
- [ ] Исправить расхождения в контракте (если есть)
- [ ] Запустить полный ETL и убедиться, что данные загружаются

### Business Logic (v_client_balances)
- [ ] Выгрузить уникальные product_name из `core.sales`
- [ ] Создать Alembic миграцию для `lookups.products`
- [ ] Наполнить справочник начальными данными
- [ ] Переписать `src/db/sql/view_client_balances.sql`
- [ ] Применить изменения через `python src/main.py --skip-extract --skip-load`
- [ ] Проверить view на аномалии (отрицательные/огромные остатки)
- [ ] Добавить мониторинг аномалий в dashboard
- [ ] Обучить пользователей добавлять новые продукты в lookup таблицу

---

## ⏱️ Оценка времени

| Задача | Оценка | Приоритет |
|:---|:---:|:---|
| **Telegram Alerts** | 1-2 часа | 🔴 Первым (блокер для prod) |
| **Validation Fix** | 2-3 часа | 🔴 Вторым (потеря данных) |
| **Business Logic** | 3-4 часа | 🟡 Третьим (качество метрик) |
| **ИТОГО** | 6-9 часов | |

---

## 🚀 Порядок выполнения

1. **Telegram Alerts** (День 1, утро) — самое быстрое, критично для observability
2. **Validation Fix** (День 1, день) — среднее по сложности, но блокирует загрузку expenses
3. **Business Logic** (День 2) — требует анализа данных, можно делать итеративно

После завершения всех трех задач — обновить `BACKLOG.md`, пометить их как `[x]` DONE.
