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

    async def load_cdc(self, table: str, col_names: List[str], rows: List[List[Any]]) -> Dict[str, int]:
        """
        Инкрементальная загрузка с использованием CDC.
        """
        log.info(f"CDC loading into {table} ({len(rows)} rows from source)")
        
        # 1. Получаем текущие хеши из БД
        existing_hashes = await self._fetch_existing_hashes(table)
        processor = CDCProcessor(existing_hashes)
        
        # 2. Обрабатываем входящие строки
        for idx, r in enumerate(rows):
            row_num = idx + 2
            try:
                # Подготовка данных (аналогично full refresh)
                full_row = list(r) + [None] * (len(col_names) - len(r))
                full_row = full_row[:len(col_names)]
                full_row_str = [str(val) if val not in (None, '') else None for val in full_row]
                
                row_hash = compute_row_hash(full_row_str)
                
                # PK identification logic (legacy_id)
                # Ищем колонку 'pk', если её нет - используем row_num как fallback, но для legacy_id лучше pk
                pk_val = None
                if 'pk' in col_names:
                    pk_idx = col_names.index('pk')
                    pk_val = full_row_str[pk_idx]
                
                if not pk_val:
                     # Если нет PK в данных, CDC работатть сложнее.
                     # но в текущем проекте у всех важных таблиц есть pk.
                     # Если pk нет, можно пропустить или использовать row_num (ненадежно)
                     continue

                row_data = {col: val for col, val in zip(col_names, full_row_str)}
                row_data['_row_index'] = row_num
                
                processor.process_row(pk_val, row_hash, row_data)
                
            except Exception as e:
                log.warning(f"Error processing row {row_num} for CDC: {e}")

        processor.finalize()
        cdc_stats = processor.get_stats()
        log.info(f"CDC Analysis for {table}: {cdc_stats}")

        # 3. Применяем изменения
        await self._apply_cdc_changes(table, processor, col_names)
        
        return cdc_stats

    async def _fetch_existing_hashes(self, table: str) -> Dict[str, str]:
        """Загружает map: legacy_id -> __row_hash."""
        query = f'SELECT legacy_id, __row_hash FROM "{table}" WHERE legacy_id IS NOT NULL'
        try:
             rows = await DBConnection.fetch(query)
             return {row['legacy_id']: row['__row_hash'] for row in rows if row['__row_hash']}
        except Exception as e:
            log.warning(f"Could not fetch hashes for {table} (maybe first run?): {e}")
            return {}

    async def _apply_cdc_changes(self, table: str, processor: CDCProcessor, col_names: List[str]):
        """Выполняет INSERT/UPDATE запросы."""
        async with await DBConnection.get_connection() as conn:
            # INSERTs
            if processor.to_insert:
                # Можно оптимизировать через copy или executemany, 
                # но для простоты и надежности пока оставим loop или executemany
                # Здесь используем простой executemany для наглядности (batch insert)
                pass 
                # TODO: Implement batch insert for performance if needed. 
                # For now implementing one-by-one to mirror legacy logic closely or simple loop
                
                for item in processor.to_insert:
                    data = item['data']
                    cols = ', '.join([f'"{c}"' for c in col_names] + ['"_row_index"', '"__row_hash"'])
                    placeholders = ', '.join([f'${i+1}' for i in range(len(col_names) + 2)])
                    values = [data.get(c) for c in col_names] + [data.get('_row_index'), item['hash']]
                    await conn.execute(f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})', *values)

            # UPDATEs
            if processor.to_update:
                pk_col = 'pk'
                for item in processor.to_update:
                    data = item['data']
                    pk_val = item['legacy_id'] # Это legacy_id
                    
                    # Динамическое построение SET
                    set_parts = []
                    vals = []
                    idx = 1
                    for c in col_names:
                        if c == pk_col: continue
                        set_parts.append(f'"{c}" = ${idx}')
                        vals.append(data.get(c))
                        idx += 1
                    
                    set_parts.append(f'"__row_hash" = ${idx}')
                    vals.append(item['hash'])
                    idx += 1
                    
                    vals.append(pk_val) # WHERE legacy_id / pk
                    
                    # ВАЖНО: В старом коде апдейт был по "pk" (который в базе мб legacy_id).
                    # В БД колонка называется `pk` или `legacy_id`?
                    # Смотрим deploy_schema: там `legacy_id` нет в CREATE TABLE, но `transform` делает SELECT legacy_id из `pk`.
                    # Значит в staging таблице колонка называется `pk`.
                    query = f'UPDATE "{table}" SET {", ".join(set_parts)} WHERE "pk" = ${idx}'
                    await conn.execute(query, *vals)

            # DELETEs
            # if processor.to_delete: ... (disabled in legacy as well)
