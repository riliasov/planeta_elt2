import yaml
import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, Any, Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # Database
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_host: Optional[str] = None
    postgres_port: int = 5432
    postgres_db: Optional[str] = None
    supabase_db_url: Optional[str] = None

    # Google Cloud
    google_service_account_json: str = "secrets/google-service-account.json"
    
    # App
    log_level: str = "INFO"
    use_staging_schema: bool = False
    
    # Database Schemas
    schema_ops: str = "ops"
    schema_staging: str = "stg_gsheets"
    schema_raw: str = "raw"
    schema_analytics: str = "analytics"
    schema_references: str = "lookups"
    
    # Sources config
    _sources_config: Dict[str, Any] = {}
    
    # DQ Settings
    dq_anomaly_threshold_small: float = 0.5  # for small tables (< 100 rows)
    dq_anomaly_threshold_large: float = 0.1  # for large tables (> 10000 rows)
    dq_history_window: int = 5    # Compare with last 5 runs

    @property
    def database_dsn(self) -> str:
        """Возвращает DSN для подключения. Приоритет у SUPABASE_DB_URL."""
        if self.supabase_db_url:
            return self.supabase_db_url
            
        if all([self.postgres_user, self.postgres_password, self.postgres_host, self.postgres_db]):
            return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            
        raise ValueError("Не заданы параметры подключения к БД (SUPABASE_DB_URL или POSTGRES_*)")

    @property
    def sources(self) -> Dict[str, Any]:
        """Lazy load sources.yml configuration."""
        if not self._sources_config:
            self._sources_config = self._load_sources()
        return self._sources_config

    def _load_sources(self) -> Dict[str, Any]:
        """Загружает конфигурацию из sources.yml."""
        root_dir = Path(__file__).resolve().parent.parent.parent
        config_path = root_dir / 'sources.yml'
        
        if not config_path.exists():
             # Fallback для тестов или если файл в текущей директории
             config_path = Path('sources.yml')
             
        if not config_path.exists():
            return {}
            
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

settings = Settings()
