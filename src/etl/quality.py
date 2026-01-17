import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from src.config.settings import settings
from src.db.connection import DBConnection

log = logging.getLogger('quality')

class QualityIssue:
    def __init__(self, table: str, issue_type: str, message: str, severity: str = 'warning'):
        self.table = table
        self.issue_type = issue_type
        self.message = message
        self.severity = severity

class DataQualityChecker:
    """Инструмент для проверки качества данных в staging таблицах."""
    
    def __init__(self):
        self.issues: List[QualityIssue] = []

    async def check_table(self, table: str, pk_field: str, critical_cols: List[str] = None):
        """Выполняет комплексную проверку таблицы."""
        log.info(f"Проверка качества данных для {table}...")
        
        # 1. Проверка на дубликаты по PK
        await self._check_duplicates(table, pk_field)
        
        # 2. Проверка на NULL в критических колонках
        if critical_cols:
            await self._check_nulls(table, critical_cols)
            
        # 3. Проверка на аномалии объема (сравнение с историей)
        await self._check_volume_anomalies(table)

    async def _check_duplicates(self, table: str, pk_field: str):
        query = f"""
            SELECT "{pk_field}", count(*) 
            FROM {table} 
            WHERE "{pk_field}" IS NOT NULL
            GROUP BY 1 HAVING count(*) > 1
        """
        try:
            rows = await DBConnection.fetch(query)
            if rows:
                msg = f"Обнаружено {len(rows)} дубликатов по ключу {pk_field}"
                self.issues.append(QualityIssue(table, 'DUPLICATES', msg, 'critical'))
                log.error(f"❌ {table}: {msg}")
        except Exception as e:
            log.warning(f"Не удалось проверить дубликаты в {table}: {e}")

    async def _check_nulls(self, table: str, columns: List[str]):
        for col in columns:
            query = f'SELECT count(*) as cnt FROM {table} WHERE "{col}" IS NULL'
            try:
                rows = await DBConnection.fetch(query)
                null_count = rows[0]['cnt'] if rows else 0
                if null_count > 0:
                    msg = f"Обнаружено {null_count} пустых значений в колонке '{col}'"
                    self.issues.append(QualityIssue(table, 'NULL_VALUES', msg, 'warning'))
                    log.warning(f"⚠ {table}: {msg}")
            except Exception as e:
                log.warning(f"Не удалось проверить NULL в {table}.{col}: {e}")

    async def _check_volume_anomalies(self, table: str):
        """Проверяет резкие скачки в количестве строк по сравнению с предыдущими запусками."""
        # Получаем количество строк в текущем staging
        count_query = f"SELECT count(*) as cnt FROM {table}"
        
        # Получаем исторические данные из elt_table_stats
        # Убираем схему для поиска в статистике
        table_base = table.split('.')[-1]
        history_query = f"""
            SELECT rows_extracted 
            FROM {settings.schema_ops}.elt_table_stats 
            WHERE table_name LIKE '%{table_base}'
            ORDER BY created_at DESC LIMIT {settings.dq_history_window}
        """
        
        try:
            curr_rows = (await DBConnection.fetch(count_query))[0]['cnt']
            hist_rows = await DBConnection.fetch(history_query)
            
            if not hist_rows or curr_rows == 0:
                return # Недостаточно данных для сравнения

            avg_hist = sum(r['rows_extracted'] for r in hist_rows) / len(hist_rows)
            
            # Адаптивный порог
            if avg_hist < 100:
                threshold = settings.dq_anomaly_threshold_small  # 0.5
            elif avg_hist > 10000:
                threshold = settings.dq_anomaly_threshold_large  # 0.1
            else:
                # Линейная интерполяция между 100 и 10000
                # Formula: y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)
                slope = (settings.dq_anomaly_threshold_large - settings.dq_anomaly_threshold_small) / (10000 - 100)
                threshold = settings.dq_anomaly_threshold_small + (avg_hist - 100) * slope

            if avg_hist > 0:
                diff_pct = abs(curr_rows - avg_hist) / avg_hist
                if diff_pct > threshold: 
                    msg = f"Аномалия объема: получено {curr_rows} строк, среднее за {len(hist_rows)} запусков — {avg_hist:.1f} (отклонение {diff_pct:.1%}, порог {threshold:.0%})"
                    self.issues.append(QualityIssue(table, 'VOLUME_ANOMALY', msg, 'warning'))
                    log.warning(f"⚠ {table}: {msg}")
        except Exception as e:
            log.warning(f"Не удалось проверить аномалии объема для {table}: {e}")

    def get_summary(self) -> Dict[str, Any]:
        return {
            'has_critical_issues': any(i.severity == 'critical' for i in self.issues),
            'issue_count': len(self.issues),
            'issues': [vars(i) for i in self.issues]
        }
