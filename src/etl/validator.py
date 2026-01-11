"""Валидатор данных на основе JSON-контрактов."""
import json
import re
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from typing import List, Dict, Any, Optional, Tuple

log = logging.getLogger('validator')


from pydantic import BaseModel, Field, field_validator
from src.utils.helpers import slugify

# Removed dataclass decorator as we are moving to Pydantic
class ValidationError(BaseModel):
    """Описание ошибки валидации."""
    row_index: int
    column: str
    value: Any
    error_type: str
    message: str


class ValidationResult(BaseModel):
    """Результат валидации набора данных."""
    is_valid: bool
    total_rows: int
    valid_rows: int
    errors: List[ValidationError] = Field(default_factory=list)
    
    @property
    def error_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.total_rows - self.valid_rows) / self.total_rows


class ContractValidator:
    """Валидатор на основе JSON-контрактов."""
    
    # Паттерны для парсинга дат
    DATE_PATTERNS = {
        'DD.MM.YYYY': r'^\d{2}\.\d{2}\.\d{4}$',
        'DD.MM.YY': r'^\d{2}\.\d{2}\.\d{2}$',
        'DD.MM.': r'^\d{2}\.\d{2}\.$',
        'DD.MM': r'^\d{2}\.\d{2}$',
        'YYYY-MM-DD': r'^\d{4}-\d{2}-\d{2}$',
    }
    
    TIME_PATTERN = r'^\d{1,2}:\d{2}(:\d{2})?$'
    
    def __init__(self, contracts_dir: Optional[Path] = None):
        if contracts_dir is None:
            contracts_dir = Path(__file__).parent.parent / 'contracts'
        self.contracts_dir = contracts_dir
        self._contracts_cache: Dict[str, dict] = {}
    
    def load_contract(self, entity_name: str) -> dict:
        """Загружает контракт по имени сущности."""
        if entity_name in self._contracts_cache:
            return self._contracts_cache[entity_name]
        
        contract_path = self.contracts_dir / f'{entity_name.lower()}.json'
        if not contract_path.exists():
            raise FileNotFoundError(f"Contract not found: {contract_path}")
        
        with open(contract_path, 'r', encoding='utf-8') as f:
            contract = json.load(f)
        
        self._contracts_cache[entity_name] = contract
        return contract
    
    def validate_row(self, row: Dict[str, Any], contract: dict, row_index: int) -> List[ValidationError]:
        """Валидирует одну строку по контракту."""
        errors = []
        
        for col_spec in contract.get('columns', []):
            col_name = col_spec['name']
            col_type = col_spec.get('type', 'string')
            required = col_spec.get('required', False)
            default = col_spec.get('default')
            formats = col_spec.get('format')
            
            # Try direct lookup first, then slugified
            if col_name in row:
                value = row[col_name]
            else:
                slug_name = slugify(col_name)
                # Handle possible duplicate suffixes (though we usually validate canonical name)
                value = row.get(slug_name)
            
            # Проверка обязательности
            if required and self._is_empty(value):
                if default is None:
                    errors.append(ValidationError(
                        row_index=row_index,
                        column=col_name,
                        value=value,
                        error_type='MISSING_REQUIRED',
                        message=f"Обязательное поле '{col_name}' пустое"
                    ))
                continue
            
            # Пропускаем валидацию типа для пустых необязательных полей
            if self._is_empty(value):
                continue
            
            # Валидация типа
            type_error = self._validate_type(value, col_type, formats, col_name, row_index)
            if type_error:
                errors.append(type_error)
        
        return errors
    
    def validate_dataset(self, rows: List[Dict[str, Any]], entity_name: str) -> ValidationResult:
        """Валидирует весь набор данных."""
        contract = self.load_contract(entity_name)
        all_errors = []
        valid_count = 0
        
        for idx, row in enumerate(rows):
            row_errors = self.validate_row(row, contract, idx)
            if not row_errors:
                valid_count += 1
            else:
                all_errors.extend(row_errors)
        
        return ValidationResult(
            is_valid=len(all_errors) == 0,
            total_rows=len(rows),
            valid_rows=valid_count,
            errors=all_errors
        )
    
    def _is_empty(self, value: Any) -> bool:
        """Проверяет, является ли значение пустым."""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == '':
            return True
        return False
    
    def _validate_type(self, value: Any, col_type: str, formats: Any, col_name: str, row_index: int) -> Optional[ValidationError]:
        """Валидирует тип значения."""
        str_value = str(value).strip()
        
        if col_type == 'string':
            return None  # Любое значение валидно как строка
        
        elif col_type == 'integer':
            try:
                int(str_value)
            except ValueError:
                return ValidationError(
                    row_index=row_index,
                    column=col_name,
                    value=value,
                    error_type='INVALID_INTEGER',
                    message=f"Значение '{value}' не является целым числом"
                )
        
        elif col_type == 'money':
            # Убираем пробелы, буквы и проверяем число
            cleaned = re.sub(r'[^\d,.-]', '', str_value)
            if cleaned:
                try:
                    float(cleaned.replace(',', '.'))
                except ValueError:
                    return ValidationError(
                        row_index=row_index,
                        column=col_name,
                        value=value,
                        error_type='INVALID_MONEY',
                        message=f"Значение '{value}' не является суммой"
                    )
        
        elif col_type == 'date':
            if not self._validate_date_format(str_value, formats):
                return ValidationError(
                    row_index=row_index,
                    column=col_name,
                    value=value,
                    error_type='INVALID_DATE',
                    message=f"Дата '{value}' не соответствует формату {formats}"
                )
        
        elif col_type == 'time':
            if not re.match(self.TIME_PATTERN, str_value):
                return ValidationError(
                    row_index=row_index,
                    column=col_name,
                    value=value,
                    error_type='INVALID_TIME',
                    message=f"Время '{value}' не соответствует формату HH:MM"
                )
        
        return None
    
    def _validate_date_format(self, value: str, formats: Any) -> bool:
        """Проверяет соответствие даты одному из форматов."""
        # Убираем возможный префикс дня недели (пн, вт, ...)
        cleaned = re.sub(r'^[а-яa-z]{2,3}\s+', '', value, flags=re.IGNORECASE).strip()
        
        if formats is None:
            formats = list(self.DATE_PATTERNS.keys())
        elif isinstance(formats, str):
            formats = [formats]
        
        for fmt in formats:
            pattern = self.DATE_PATTERNS.get(fmt)
            if pattern and re.match(pattern, cleaned):
                return True
        
        return False


def validate_staging_table(table_name: str, entity_name: str, rows: List[Dict[str, Any]]) -> ValidationResult:
    """Утилита для валидации staging таблицы."""
    validator = ContractValidator()
    result = validator.validate_dataset(rows, entity_name)
    
    if result.is_valid:
        log.info(f"✓ {table_name}: все {result.total_rows} строк валидны")
    else:
        log.warning(f"⚠ {table_name}: {result.valid_rows}/{result.total_rows} валидных строк ({result.error_rate:.1%} ошибок)")
        # Логируем первые 5 ошибок
        for err in result.errors[:5]:
            log.warning(f"  [Строка {err.row_index}]: {err.message}")
    
    return result
