import asyncpg
import logging
from typing import Optional
from src.config.settings import settings

log = logging.getLogger('db')

class DBConnection:
    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def get_pool(cls) -> asyncpg.Pool:
        """Возвращает пул соединений, создавая его при необходимости."""
        if cls._pool is None:
            log.info("Инициализация пула соединений БД...")
            # Pgbouncer требует session mode или statement_cache_size=0
            cls._pool = await asyncpg.create_pool(
                dsn=settings.database_dsn,
                statement_cache_size=0,
                min_size=1,
                max_size=10
            )
        return cls._pool

    @classmethod
    async def close(cls):
        """Закрывает пул соединений."""
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            log.info("Пул соединений закрыт.")

    @classmethod
    async def fetch(cls, query: str, *args):
        pool = await cls.get_pool()
        async with pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def execute(cls, query: str, *args):
        pool = await cls.get_pool()
        async with pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def get_connection(cls):
        """Возвращает контекстный менеджер для получения соединения."""
        pool = await cls.get_pool()
        return pool.acquire()
