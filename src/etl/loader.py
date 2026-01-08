import logging
import asyncio
from typing import List, Dict, Any
from src.db.connection import DBConnection
from src.etl.cdc_processor import compute_row_hash, CDCProcessor

log = logging.getLogger('loader')

class DataLoader:
    def __init__(self):
        pass

    async def load_full_refresh(self, table: str, col_names: List[str], rows: List[List[Any]]) -> Dict[str, int]:
        """
        Полная перезагрузка таблицы: TRUNCATE + INSERT.
        """
        log.info(f"Full Refresh loading into {table} ({len(rows)} rows)")
        stats = {'inserted': 0, 'errors': 0}
        
        async with await DBConnection.get_connection() as conn:
            async with conn.transaction():
                # 1. Truncate
                await conn.execute(f'TRUNCATE TABLE "{table}"')
                
                # 2. Подготовка данных
                prepared_rows = []
                target_cols = col_names + ["_row_index", "__row_hash"]
                
                for idx, r in enumerate(rows):
                    row_num = idx + 2 # Заголовок на 1 строке
                    try:
                         # Выравнивание и приведение к строке
                        full_row = list(r) + [None] * (len(col_names) - len(r))
                        full_row = full_row[:len(col_names)]
                        full_row_str = [str(val) if val not in (None, '') else None for val in full_row]
                        
                        row_hash = compute_row_hash(full_row_str)
                        prepared_rows.append(tuple(full_row_str + [row_num, row_hash]))
                    except Exception as e:
                        log.warning(f"Error preparing row {row_num}: {e}")
                        stats['errors'] += 1
                
                # 3. Mass Insert (Copy)
                if prepared_rows:
                    await conn.copy_records_to_table(
                        table,
                        records=prepared_rows,
                        columns=target_cols
                    )
                    stats['inserted'] = len(prepared_rows)
                    
        log.info(f"Full Refresh finished for {table}: {stats}")
        return stats

    async def load_cdc(self, table: str, col_names: List[str], rows: List[List[Any]], pk_field: str = '__row_hash') -> Dict[str, int]:
        """
        Инкрементальная загрузка с использованием CDC.
        """
        log.info(f"CDC loading into {table} ({len(rows)} rows from source) [PK: {pk_field}]")
        
        # 1. Получаем текущие хеши из БД
        existing_hashes = await self._fetch_existing_hashes(table, pk_field)
        processor = CDCProcessor(existing_hashes)
        
        # 2. Обрабатываем входящие строки
        for idx, r in enumerate(rows):
            row_num = idx + 2
            try:
                # Подготовка данных
                full_row = list(r) + [None] * (len(col_names) - len(r))
                full_row = full_row[:len(col_names)]
                full_row_str = [str(val) if val not in (None, '') else None for val in full_row]
                
                row_hash = compute_row_hash(full_row_str)
                
                # PK identification logic
                if pk_field == '__row_hash':
                    pk_val = row_hash
                elif pk_field in col_names:
                    pk_idx = col_names.index(pk_field)
                    pk_val = full_row_str[pk_idx]
                else:
                    # Fallback if PK field configured but not found in columns (unlikely if schema matches)
                    # For safety, skip or log warning
                    pk_val = None

                if not pk_val:
                     continue

                row_data = {col: val for col, val in zip(col_names, full_row_str)}
                row_data['_row_index'] = row_num
                
                processor.process_row(pk_val, row_hash, row_data)
                
            except Exception as e:
                log.warning(f"Error processing row {row_num} for CDC: {e}")

        processor.finalize()
        cdc_stats = processor.get_stats()
        
        # 3. Применяем изменения
        await self._apply_cdc_changes(table, processor, col_names, pk_field)
        
        return cdc_stats

    async def calculate_changes(self, table: str, col_names: List[str], rows: List[List[Any]], pk_field: str = '__row_hash') -> Dict[str, int]:
        """Вычисляет статистику изменений без применения (для dry-run)."""
        log.info(f"DRY-RUN: Calculating changes for {table} [PK: {pk_field}]")
        
        existing_hashes = await self._fetch_existing_hashes(table, pk_field)
        processor = CDCProcessor(existing_hashes)
        
        for idx, r in enumerate(rows):
            row_num = idx + 2
            try:
                full_row = list(r) + [None] * (len(col_names) - len(r))
                full_row = full_row[:len(col_names)]
                full_row_str = [str(val) if val not in (None, '') else None for val in full_row]
                
                row_hash = compute_row_hash(full_row_str)
                
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
                log.warning(f"Error processing row {row_num} for dry-run: {e}")

        processor.finalize()
        return processor.get_stats()

    async def _fetch_existing_hashes(self, table: str, pk_field: str) -> Dict[str, str]:
        # Динамический выбор PK
        try:
            query = f'SELECT "{pk_field}" as pk, __row_hash FROM "{table}" WHERE "{pk_field}" IS NOT NULL'
            rows = await DBConnection.fetch(query)
            return {str(row['pk']): row['__row_hash'] for row in rows if row['__row_hash']}
        except Exception as e:
            log.warning(f"Could not fetch hashes for {table} (column {pk_field} missing?): {e}")
            return {}

    async def _apply_cdc_changes(self, table: str, processor: CDCProcessor, col_names: List[str], pk_field: str):
        """Выполняет INSERT/UPDATE запросы."""
        async with await DBConnection.get_connection() as conn:
            # INSERTs
            if processor.to_insert:
                for item in processor.to_insert:
                    data = item['data']
                    cols = ', '.join([f'"{c}"' for c in col_names] + ['"_row_index"', '"__row_hash"'])
                    placeholders = ', '.join([f'${i+1}' for i in range(len(col_names) + 2)])
                    values = [data.get(c) for c in col_names] + [data.get('_row_index'), item['hash']]
                    await conn.execute(f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})', *values)

            # UPDATEs
            if processor.to_update:
                for item in processor.to_update:
                    data = item['data']
                    pk_val = item['legacy_id']
                    
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
                    
                    vals.append(pk_val)
                    
                    query = f'UPDATE "{table}" SET {", ".join(set_parts)} WHERE "{pk_field}" = ${idx}'
                    await conn.execute(query, *vals)

            # DELETEs
            if processor.to_delete:
                del_query = f'DELETE FROM "{table}" WHERE "{pk_field}" = $1'
                for pk_val in processor.to_delete:
                    await conn.execute(del_query, pk_val)
                
                log.info(f"Deleted {len(processor.to_delete)} rows from {table}")
