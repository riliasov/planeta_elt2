🔍 Экспертный аудит ELT-пайплайна Planeta
📋 Executive Summary
Проект представляет собой асинхронный ELT-пайплайн для синхронизации данных Google Sheets → Supabase PostgreSQL. Архитектура следует современным практикам Data Engineering, но имеет критические риски перед продакшеном.
Общая оценка зрелости: 🟡 Pre-Production (65/100)

🚨 КРИТИЧЕСКИЕ ПРОБЛЕМЫ (Блокеры продакшена)
1. Отсутствие идемпотентности трансформаций 🔴 CRITICAL
Файлы: src/db/sql/transform_*.sql
Проблема:
sql-- transform_sales.sql
INSERT INTO core.sales (...)
SELECT DISTINCT ON (legacy_id) ...
ON CONFLICT (legacy_id) DO UPDATE SET ...
Риск:

При повторном запуске трансформации данные дублируются или перезаписываются некорректно
DISTINCT ON (legacy_id) + md5() для генерации legacy_id может дать коллизии при изменении порядка строк
Нет защиты от race conditions при параллельных запусках

Решение:
sql-- 1. Использовать MERGE (PostgreSQL 15+) вместо INSERT ... ON CONFLICT
MERGE INTO core.sales AS target
USING (
  SELECT DISTINCT ON (record_id) ... 
  FROM stg_gsheets.sales_cur
  ORDER BY record_id, sheet_updated_at DESC
) AS source
ON target.legacy_id = source.legacy_id
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...;

-- 2. Добавить защиту от дублей через временную таблицу
CREATE TEMP TABLE sales_dedup AS
SELECT DISTINCT ON (record_id) *
FROM stg_gsheets.sales_cur
ORDER BY record_id, sheet_updated_at DESC;

INSERT INTO core.sales ...
FROM sales_dedup;
Приоритет: 🔥 P0 (Делать перед первым продакшен-запуском)

2. Конфликт стратегий CDC 🔴 CRITICAL
Файлы: sources.yml, src/etl/loader.py
Противоречие:
yaml# sources.yml
defaults:
  change_detection_strategy: "hash"  # Декларация
  pk: "__row_hash"  # Но затем переопределяется

sheets:
  - id: clients_cur
    pk: "record_id"  # UUID из GAS
Проблема:

В коде используется два PK одновременно: record_id (stable UUID) и __row_hash (content-based)
При load_cdc() непонятно, какой PK приоритетнее:

python  # loader.py:74
  if pk_field == '__row_hash':
      pk_val = row_hash
  elif pk_field in col_names:
      pk_val = full_row_str[pk_idx]

Риск data loss: если record_id меняется в Sheets (пересоздание строки), старая запись останется в БД как "мертвая"

Решение:
yaml# СТРАТЕГИЯ 1: UUID-based CDC (рекомендуется)
defaults:
  change_detection_strategy: "uuid"
  pk: "record_id"
  compute_row_hash: true  # Для обнаружения изменений контента

# СТРАТЕГИЯ 2: Hash-based CDC (legacy)
defaults:
  change_detection_strategy: "hash"
  pk: "__row_hash"
  enable_hard_delete: false  # row_hash нестабилен!
python# loader.py - унифицировать логику
async def load_cdc(self, table, col_names, rows, pk_field='record_id'):
    # ВСЕГДА использовать pk_field из config
    # __row_hash только для детекции изменений (WHERE pk = X AND hash != Y)
Приоритет: 🔥 P0

3. Race Conditions в ProcessLock 🟠 HIGH
Файл: src/utils/process.py
Проблема:
python# process.py:47
if self.lock_file.exists():
    with open(self.lock_file, "r") as f:
        old_pid = int(f.read().strip())
    # RACE: Между exists() и open() другой процесс может удалить файл
Атака:

Процесс A проверяет exists() → True
Процесс B удаляет lock-файл и создает новый
Процесс A читает PID процесса B, думает что это старый процесс
Процесс A убивает процесс B (если --kill-conflicts)

Решение:
pythonimport fcntl  # POSIX file locking

class ProcessLock:
    def __init__(self, name: str):
        self.lock_file = Path(f"/var/lock/elt_{name}.lock")
        self.lock_fd = None
    
    def check_and_lock(self):
        self.lock_fd = open(self.lock_file, 'w')
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
        except BlockingIOError:
            raise RuntimeError("Pipeline already running")
Альтернатива (если Windows): Redis-based distributed lock или PostgreSQL advisory locks.
Приоритет: 🔥 P0 (если используется --kill-conflicts)

4. SQL Injection в динамических запросах 🟠 HIGH
Файл: src/etl/loader.py
Уязвимость:
python# loader.py:189
query = f'UPDATE {target_table_sql} SET {", ".join(set_parts)} WHERE "{pk_field}" = ${idx}'
Сценарий атаки:
yaml# sources.yml (если злоумышленник получит доступ)
sheets:
  - target_table: "core.sales; DROP TABLE core.clients--"
Текущая защита:
pythonself._ident_pattern = re.compile(r'^[a-zA-Z0-9_.]+$')  # ✅ Есть валидация
Проблема: Точка . разрешена для schema.table, но может использоваться в атаках:
sql-- Пример: table = "core.sales WHERE 1=1; --"
UPDATE core.sales WHERE 1=1; -- SET ...
Решение:
pythondef _validate_identifier(self, ident: str) -> str:
    parts = ident.split('.')
    if len(parts) > 2:
        raise ValueError(f"Invalid identifier: {ident}")
    
    for part in parts:
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', part):  # Начало с буквы!
            raise ValueError(f"Invalid identifier part: {part}")
    return ident
Приоритет: 🟠 P1 (Доработать перед продакшеном)

⚠️ АРХИТЕКТУРНЫЕ РИСКИ
5. Отсутствие версионирования схемы БД 🟡 MEDIUM
Проблема:

Миграции в src/db/migrations/ не управляются инструментом (нет Alembic/Liquibase)
Невозможно откатить схему назад
Нет истории изменений схемы

Решение:
bash# Внедрить Alembic
pip install alembic
alembic init alembic

# alembic/versions/001_initial_schema.py
def upgrade():
    op.execute(open('src/db/sql/init_layered_architecture.sql').read())

def downgrade():
    op.drop_schema('core', cascade=True)
Приоритет: 🟡 P2 (Важно для долгосрочной поддержки)

6. Хрупкость маппинга колонок 🟡 MEDIUM
Файл: src/etl/extractor.py
Проблема:
python# extractor.py:88
def _normalize_headers(self, headers, table_name, mapping):
    if mapping and h in mapping:
        col_name = mapping[h]
    else:
        col_name = slugify(h)  # "Полная Стоимость" → "polnaya_stoimost"
Риск:

Если пользователь в Sheets переименует колонку с "Полная Стоимость" → "Полная цена", данные перестанут загружаться
Нет явного контракта между Sheets и БД

Решение:
yaml# sources.yml - Явный маппинг ОБЯЗАТЕЛЕН
sheets:
  - id: sales_cur
    column_mapping:
      "Полная Стоимость": "full_price"  # Явно
      "Дата": "sale_date"
      "Клиент": "client_name"
    strict_mode: true  # Ошибка, если колонка не в маппинге
python# extractor.py
if strict_mode and h not in mapping:
    raise ValueError(f"Unmapped column '{h}' in {table_name}")
Приоритет: 🟡 P2

7. Отсутствие мониторинга качества данных 🟡 MEDIUM
Файл: src/etl/pipeline.py
Проблема:
python# pipeline.py:126
if val_result.errors > 20:
    raise ValueError("КРИТИЧНО: >20 ошибок")
Недостатки:

Жесткий порог 20 не учитывает размер таблицы (20 ошибок из 100 строк vs 20 из 10 000)
Нет трендов (если ошибок становится больше с каждым запуском → проблема)
Нет алертов в Telegram/Email

Решение:
python# Динамический порог
error_rate = len(val_result.errors) / len(rows)
if error_rate > 0.05:  # 5% ошибок
    log.critical(f"High error rate: {error_rate:.1%}")
    await send_telegram_alert(f"⚠️ {table}: {error_rate:.1%} ошибок")
sql-- Дашборд качества
CREATE VIEW ops.data_quality_trends AS
SELECT 
  DATE_TRUNC('day', created_at) as date,
  table_name,
  SUM(validation_errors)::FLOAT / NULLIF(SUM(rows_extracted), 0) as error_rate
FROM ops.elt_table_stats
GROUP BY 1, 2
ORDER BY 1 DESC;
Приоритет: 🟡 P2

🛠️ ТЕХНИЧЕСКИЙ ДОЛГ
8. Дублирование трансформационной логики 🟢 LOW
Файлы: transform_sales.sql, transform_schedule.sql
Проблема:
sql-- Копипаста парсинга дат
CASE 
    WHEN "data"::text ~ '\d{2}\.\d{2}\.\d{2}' 
    THEN TO_DATE(substring("data"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')
    -- ... 5 вариантов
END
Решение:
sql-- Создать функцию
CREATE OR REPLACE FUNCTION parse_russian_date(val TEXT) 
RETURNS DATE AS $$
BEGIN
  -- Логика парсинга
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Использовать
INSERT INTO core.sales (..., date, ...)
SELECT ..., parse_russian_date("data"), ...
Приоритет: 🟢 P3

9. Отсутствие интеграционных тестов 🟢 LOW
Проблема:

Только unit-тесты (test_validator.py, test_cdc.py)
Нет тестов полного цикла E2E с реальной БД

Решение:
python# tests/integration/test_full_pipeline.py
@pytest.mark.integration
async def test_full_refresh_pipeline():
    # 1. Подготовка test DB
    await create_test_schema()
    
    # 2. Мок Google Sheets
    mock_data = [["Иванов", "79991234567", "Зал"]]
    
    # 3. Запуск пайплайна
    pipeline = ELTPipeline()
    await pipeline.run(full_refresh=True, scope='current')
    
    # 4. Проверка результата
    rows = await DBConnection.fetch("SELECT * FROM stg_gsheets.clients_cur")
    assert len(rows) == 1
    assert rows[0]['klient'] == "Иванов"
Приоритет: 🟢 P3

📊 ПЛАН ВНЕДРЕНИЯ (Production Readiness Roadmap)
🔥 PHASE 0: Критические фиксы (1-2 недели)
ЗадачаПриоритетВремяРиск если не сделатьFix #1: Идемпотентность трансформаций (MERGE)P03dData corruption при rerunFix #2: Унификация CDC стратегии (UUID only)P02dData loss, дублиFix #3: POSIX file locks вместо PID-файловP01dКонкурентные запускиFix #4: Усилить валидацию SQL идентификаторовP11dSQL injection
Критерий выхода: Все P0 задачи закрыты + прогон на staging окружении.

🟡 PHASE 1: Стабилизация (2-3 недели)
ЗадачаПриоритетВремяВнедрить Alembic для управления миграциямиP23dДобавить strict_mode для column mappingP22dСоздать Data Quality Dashboard (Grafana/Streamlit)P24dНастроить Telegram-алерты при критических ошибкахP21dДокументация архитектуры (ADR, схемы потоков)P22d
Критерий выхода: Мониторинг работает, алерты настроены, документация актуальна.

🟢 PHASE 2: Оптимизация (опционально)
ЗадачаПриоритетВремяВынести парсинг дат в SQL функцииP32dИнтеграционные E2E тестыP35dДобавить партиционирование ops.elt_runs по датеP31dCI/CD: автодеплой в staging при merge в mainP33d

🎯 РЕКОМЕНДАЦИИ ПО АРХИТЕКТУРЕ
✅ Что сделано хорошо:

Слоистая архитектура БД (raw → stg_gsheets → core) — best practice
Асинхронность (asyncpg, asyncio) — отличная производительность
Validation Contracts (Pydantic) — явный контракт данных
Audit Trail (raw.sheets_dump) — можно восстановить любой запуск
Операционные метрики (ops.elt_runs, ops.elt_table_stats) — хороший базис для мониторинга

⚠️ Что улучшить:

Нет распределенных блокировок → используйте PostgreSQL Advisory Locks:

python   # Вместо file locks
   await conn.execute("SELECT pg_advisory_lock(hashtext('elt_pipeline'))")

Нет retry с exponential backoff на уровне БД → добавьте:

python   @with_retry(max_attempts=3, exceptions=(asyncpg.PostgresConnectionError,))
   async def execute_query(...):

Нет Circuit Breaker для Google Sheets API → если квоты исчерпаны, пайплайн должен остановиться gracefully, а не ретраиться бесконечно.
Нет observability → добавьте:

Structured logging (JSON) вместо plaintext
OpenTelemetry для трассировки запросов
Prometheus метрики (pipeline_duration_seconds, rows_processed_total)




🚀 КРИТЕРИИ ГОТОВНОСТИ К ПРОДАКШЕНУ
✅ Must-Have (Blocking):

 Фикс #1: MERGE вместо INSERT ON CONFLICT
 Фикс #2: Единая CDC стратегия (UUID)
 Фикс #3: Distributed locks
 Фикс #4: SQL injection защита
 Staging окружение с реальными данными (10k+ строк)
 Runbook для инцидентов (что делать если пайплайн упал)

🟡 Should-Have (Желательно):

 Data Quality Dashboard
 Telegram алерты
 Документация API и схемы БД

🟢 Nice-to-Have (Опционально):

 E2E тесты
 CI/CD автодеплой
 Партиционирование логов


📚 ДОПОЛНИТЕЛЬНЫЕ РЕСУРСЫ
Recommended Reading:

"Data Pipelines Pocket Reference" (James Densmore) — best practices для ELT
"Designing Data-Intensive Applications" (Martin Kleppmann) — идемпотентность, distributed locks
Google SRE Book — runbooks, incident management

Tools:

Great Expectations — data quality testing framework (альтернатива Pydantic для валидации)
DBT — трансформации с версионированием и тестами (альтернатива SQL файлам)
Dagster/Prefect — оркестрация с built-in monitoring (альтернатива ручному ELTPipeline)