"""add raw cleanup function

Revision ID: a4c281dc41c3
Revises: 57df7a9f2a4b
Create Date: 2026-01-17 21:19:17.263034

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a4c281dc41c3'
down_revision: Union[str, Sequence[str], None] = '57df7a9f2a4b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
    -- Добавить TTL политику
    CREATE INDEX IF NOT EXISTS idx_sheets_dump_created_at ON raw.sheets_dump(created_at);

    -- Функция для автоочистки
    CREATE OR REPLACE FUNCTION raw.cleanup_old_dumps(retention_days INT DEFAULT 30)
    RETURNS INT AS $$
    DECLARE
        deleted_count INT;
    BEGIN
        DELETE FROM raw.sheets_dump 
        WHERE created_at < NOW() - (retention_days || ' days')::INTERVAL;
        
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        RETURN deleted_count;
    END;
    $$ LANGUAGE plpgsql;
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("""
    DROP FUNCTION IF EXISTS raw.cleanup_old_dumps(INT);
    DROP INDEX IF EXISTS raw.idx_sheets_dump_created_at;
    """)
