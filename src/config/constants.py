"""Константы проекта pl-etl-core."""

# Database
DB_BATCH_SIZE = 1000
DB_CONNECTION_POOL_SIZE = 5
DB_MAX_OVERFLOW = 10

# Data Processing
DATE_FORMAT = '%d.%m.%Y'
DATETIME_FORMAT = '%d.%m.%Y %H:%M:%S'
DEFAULT_ENCODING = 'utf-8'

# Timeouts & Retry
SHEETS_READ_TIMEOUT = 30
DB_QUERY_TIMEOUT = 60
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 2  # секунды (exponential backoff)

# Column keywords для автоматического определения типов
NUMERIC_KEYWORDS = [
    'stoimost', 'summa', 'kolichestvo', 'bonus',
    'nalichnye', 'perevod', 'terminal', 'vdolg',
    'zp', 'oplata', 'stavka', 'spisano', 'god',
    'mesyats', 'chasy', 'cena', 'skidka', 'price',
    'quantity', 'amount', 'balance', 'debt'
]

DATE_KEYWORDS = ['data', 'date', 'zapis', 'den', 'dob', 'birthday', 'created', 'updated']

BOOLEAN_COLUMNS = [
    'probili_na_evotore', 'vnesli_v_crm',
    'relevant', 'zamena', 'active', 'is_active'
]

SERVICE_COLUMNS = [
    'source_row_id', 'row_hash', '__row_hash', 
    'id', 'imported_at', '_row_index', '_loaded_at'
]
