import logging
import asyncio
import re
from typing import List, Dict, Any, Tuple, Iterable, Optional
from src.db.connection import DBConnection
from src.config.settings import settings
from src.utils.cleaning import normalize_numeric_string
from src.etl.cdc_processor import compute_row_hash, CDCProcessor

log = logging.getLogger('loader')

class DataLoader:
    def __init__(self):
        self.schema_prefix = 'staging.' if settings.use_staging_schema else ''
        # Строгая валидация: начинается с буквы, только буквы, цифры и подчеркивание.
        self._single_ident_pattern = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*$')

    def _validate_identifier(self, ident: str) -> str:
        """Проверяет идентификатор (таблица/колонка) на наличие инъекций."""
        if not ident:
            raise ValueError("Идентификатор не может быть пустым")
        
        # Если это полное имя таблицы со схемой (schema.table)
        if '.' in ident:
            parts = ident.split('.')
            if len(parts) != 2:
                raise ValueError(f"Недопустимый формат идентификатора (ожидалось schema.table): {ident}")
            for part in parts:
                if not self._single_ident_pattern.match(part):
                    raise ValueError(f"Недопустимая часть идентификатора: {part}")
        else:
            # Одиночный идентификатор (колонка или таблица без схемы)
            if not self._single_ident_pattern.match(ident):
                raise ValueError(f"Недопустимый идентификатор: {ident}")
        
        return ident

    def _format_table_name(self, table: str) -> str:
        """Форматирует имя таблицы, учитывая схему (stg.table -> "stg"."table")."""
        table = self._validate_identifier(table)
        if '.' in table:
            schema, tbl = table.split('.', 1)
            return f'"{schema}"."{tbl}"'
        
        # Если схемы нет в имени, используем префикс из настроек (если есть)
        return f'{self.schema_prefix}"{table}"'

    def _prepare_row(self, r: List[Any], col_names: List[str], row_num: int) -> Tuple[List[str], str]:
        """Унифицированная подготовка строки: выравнивание, очистка, хеширование."""
        # Выравнивание и приведение к строке
        full_row = list(r) + [None] * (len(col_names) - len(r))
        full_row = full_row[:len(col_names)]
        
        # Нормализация данных (всё в строки для хеширования)
        full_row_str = [normalize_numeric_string(val) for val in full_row]
        row_hash = compute_row_hash(full_row_str)
        
        return full_row_str, row_hash

    async def load_full_refresh(self, table: str, col_names: List[str], rows: Iterable[List[Any]], row_count: Optional[int] = None) -> Dict[str, int]:
        """Полная перезагрузка таблицы: TRUNCATE + INSERT."""
        if '.' not in table:
             table = self._validate_identifier(table)
        
        target_table_sql = self._format_table_name(table)
        validated_cols = [self._validate_identifier(c) for c in col_names]
        
        # Determine count for logging (handle Generator)
        count_str = f"{row_count} строк" if row_count is not None else "? строк"
        if row_count is None and isinstance(rows, list):
             count_str = f"{len(rows)} строк"

        log.info(f"Начало полной перезагрузки {target_table_sql} ({count_str})")
        stats = {'inserted': 0, 'errors': 0}
        
        async with await DBConnection.get_connection() as conn:
            async with conn.transaction():
                await conn.execute(f'TRUNCATE TABLE {target_table_sql}')
                
                prepared_records = []
                target_cols = validated_cols + ["_row_index", "__row_hash"]
                
                for idx, r in enumerate(rows):
                    row_num = idx + 2
                    try:
                        full_row_str, row_hash = self._prepare_row(r, col_names, row_num)
                        prepared_records.append(tuple(full_row_str + [row_num, row_hash]))
                    except Exception as e:
                        log.warning(f"Ошибка подготовки строки {row_num}: {e}")
                        stats['errors'] += 1
                
                if prepared_records:
                    if '.' in table:
                        target_schema = table.split('.', 1)[0]
                        target_table_only = table.split('.', 1)[1]
                    else:
                        target_schema = self.schema_prefix.replace('.', '') if self.schema_prefix else None
                        target_table_only = table
                        
                    await conn.copy_records_to_table(
                        target_table_only,
                        schema_name=target_schema,
                        records=prepared_records,
                        columns=target_cols
                    )
                    stats['inserted'] = len(prepared_records)
                    
        log.info(f"Полная перезагрузка {table} завершена: {stats}")
        return stats

    async def load_cdc(self, table: str, col_names: List[str], rows: Iterable[List[Any]], pk_field: str = '__row_hash', row_count: Optional[int] = None) -> Dict[str, int]:
        """Инкрементальная загрузка с использованием CDC."""
        if '.' not in table:
             table = self._validate_identifier(table)
        pk_field = self._validate_identifier(pk_field)
        target_table_sql = self._format_table_name(table)
        
        count_str = f"{row_count} строк" if row_count is not None else "? строк"
        if row_count is None and isinstance(rows, list):
             count_str = f"{len(rows)} строк"

        log.info(f"CDC загрузка в {target_table_sql} ({count_str} из источника) [PK: {pk_field}]")
        
        existing_hashes = await self._fetch_existing_hashes(table, pk_field)
        processor = CDCProcessor(existing_hashes)
        
        for idx, r in enumerate(rows):
            row_num = idx + 2
            try:
                full_row_str, row_hash = self._prepare_row(r, col_names, row_num)
                
                # PK identification
                if pk_field == '__row_hash':
                    pk_val = row_hash
                elif pk_field in col_names:
                    pk_idx = col_names.index(pk_field)
                    pk_val = full_row_str[pk_idx]
                else:
                    pk_val = None

                if not pk_val:
                     continue

                row_data = {col: val for col, val in zip(col_names, full_row_str)}
                row_data['_row_index'] = row_num
                
                processor.process_row(pk_val, row_hash, row_data)
            except Exception as e:
                log.warning(f"Ошибка обработки строки {row_num} для CDC: {e}")

        processor.finalize()
        cdc_stats = processor.get_stats()
        await self._apply_cdc_changes(table, processor, col_names, pk_field)
        return cdc_stats

    async def calculate_changes(self, table: str, col_names: List[str], rows: Iterable[List[Any]], pk_field: str = '__row_hash', row_count: Optional[int] = None) -> Dict[str, int]:
        """Вычисляет статистику изменений без применения (для dry-run)."""
        if '.' not in table:
             table = self._validate_identifier(table)
        target_table_sql = self._format_table_name(table)
        pk_field = self._validate_identifier(pk_field)
        
        count_str = f"{row_count} строк" if row_count is not None else "? строк"
        if row_count is None and isinstance(rows, list):
             count_str = f"{len(rows)} строк"

        log.info(f"🔍 [DRY-RUN] Расчет изменений для {target_table_sql} ({count_str}) [PK: {pk_field}]")
        
        existing_hashes = await self._fetch_existing_hashes(table, pk_field)
        processor = CDCProcessor(existing_hashes)
        
        for idx, r in enumerate(rows):
            row_num = idx + 2
            try:
                full_row_str, row_hash = self._prepare_row(r, col_names, row_num)
                
                if pk_field == '__row_hash':
                    pk_val = row_hash
                elif pk_field in col_names:
                    pk_idx = col_names.index(pk_field)
                    pk_val = full_row_str[pk_idx]
                else:
                    pk_val = None
                
                if not pk_val:
                    continue

                row_data = {col: val for col, val in zip(col_names, full_row_str)}
                processor.process_row(pk_val, row_hash, row_data)
            except Exception as e:
                log.warning(f"Ошибка обработки строки {row_num} (dry-run): {e}")

        processor.finalize()
        return processor.get_stats()

    async def _fetch_existing_hashes(self, table: str, pk_field: str) -> Dict[str, str]:
        # table и pk_field уже валидированы выше
        # table и pk_field уже валидированы выше (в вызывающем методе) или должны быть здесь
        target_table_sql = self._format_table_name(table)
        try:
            query = f'SELECT "{pk_field}" as pk, __row_hash FROM {target_table_sql} WHERE "{pk_field}" IS NOT NULL'
            rows = await DBConnection.fetch(query)
            return {str(row['pk']): row['__row_hash'] for row in rows if row['__row_hash']}
        except Exception as e:
            log.warning(f"Не удалось получить хеши для {table} (колонка {pk_field} отсутствует?): {e}")
            return {}

    async def _apply_cdc_changes(self, table: str, processor: CDCProcessor, col_names: List[str], pk_field: str):
        """Выполняет INSERT/UPDATE/DELETE запросы."""
        if '.' not in table:
             table = self._validate_identifier(table)
        target_table_sql = self._format_table_name(table)
        validated_cols = [self._validate_identifier(c) for c in col_names]
        pk_field = self._validate_identifier(pk_field)

        async with await DBConnection.get_connection() as conn:
            # INSERTs
            if processor.to_insert:
                cols_str = ', '.join([f'"{c}"' for c in validated_cols] + ['"_row_index"', '"__row_hash"'])
                placeholders = ', '.join([f'${i+1}' for i in range(len(validated_cols) + 2)])
                insert_query = f'INSERT INTO {target_table_sql} ({cols_str}) VALUES ({placeholders})'
                
                for item in processor.to_insert:
                    data = item['data']
                    values = [data.get(c) for c in col_names] + [data.get('_row_index'), item['hash']]
                    await conn.execute(insert_query, *values)

            # UPDATEs
            if processor.to_update:
                for item in processor.to_update:
                    data = item['data']
                    pk_val = item['pk']
                    
                    set_parts = []
                    vals = []
                    idx = 1
                    for c in col_names:
                        if c == pk_field: continue
                        set_parts.append(f'"{c}" = ${idx}')
                        vals.append(data.get(c))
                        idx += 1
                    
                    set_parts.append(f'"__row_hash" = ${idx}')
                    vals.append(item['hash'])
                    idx += 1
                    vals.append(pk_val) # PK for WHERE
                    
                    query = f'UPDATE {target_table_sql} SET {", ".join(set_parts)} WHERE "{pk_field}" = ${idx}'
                    await conn.execute(query, *vals)

            # DELETEs
            if processor.to_delete:
                del_query = f'DELETE FROM {target_table_sql} WHERE "{pk_field}" = $1'
                for pk_val in processor.to_delete:
                    await conn.execute(del_query, pk_val)
                log.info(f"Удалено {len(processor.to_delete)} строк из {table}")
