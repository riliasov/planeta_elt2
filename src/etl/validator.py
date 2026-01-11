"""Валидатор данных на основе JSON-контрактов."""
import json
import re
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Type, Union
from pydantic import BaseModel, Field, create_model, validator, ValidationError as PydanticValidationError, AliasChoices
from src.utils.helpers import slugify

log = logging.getLogger('validator')

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
    """Валидатор на основе JSON-контрактов и динамических Pydantic моделей."""
    
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
        self._models_cache: Dict[str, Type[BaseModel]] = {}

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

    def _get_model_for_contract(self, entity_name: str) -> Type[BaseModel]:
        """Динамически создает Pydantic модель для контракта."""
        if entity_name in self._models_cache:
            return self._models_cache[entity_name]

        contract = self.load_contract(entity_name)
        fields = {}
        
        for col in contract.get('columns', []):
            name = col['name']
            col_type = col.get('type', 'string')
            required = col.get('required', False)
            default = col.get('default')

            # Определение типа для Pydantic
            py_type: Any = Any
            if col_type == 'integer':
                py_type = Optional[int] if not required else int
            elif col_type == 'string':
                py_type = Optional[str] if not required else str
            
            # Для сложных типов (date, money, time) пока оставляем Any и проверяем валидаторами
            # чтобы сохранить специфическую логику очистки/парсинга
            
            # Используем AliasChoices для поддержки и оригинального имени, и слагифицированного
            slug_name = slugify(name)
            if slug_name != name:
                fields[name] = (py_type, Field(default if not required else ..., validation_alias=AliasChoices(name, slug_name)))
            else:
                fields[name] = (py_type, default if not required else ...)

        # Создаем модель
        model = create_model(f"Dynamic_{entity_name}", **fields)
        self._models_cache[entity_name] = model
        return model

    def validate_row(self, row: Dict[str, Any], contract: dict, row_index: int) -> List[ValidationError]:
        """Валидирует одну строку. Частично через Pydantic, частично через кастомную логику (для сложных типов)."""
        errors = []
        
        # 1. Pydantic валидация (базовые типы)
        model = self._get_model_for_contract(contract.get('name', 'unknown'))
        try:
            # Используем model_validate или parse_obj (в зависимости от версии)
            model(**row)
        except PydanticValidationError as e:
            for error in e.errors():
                # Маппинг ошибок Pydantic в наш формат
                col = error['loc'][0]
                errors.append(ValidationError(
                    row_index=row_index,
                    column=str(col),
                    value=row.get(str(col)),
                    error_type=error['type'],
                    message=error['msg']
                ))

        # 2. Кастомная валидация сложных типов (date, money, time), которые Pydantic может пропустить
        for col_spec in contract.get('columns', []):
            col_name = col_spec['name']
            col_type = col_spec.get('type', 'string')
            formats = col_spec.get('format')
            
            value = row.get(col_name) or row.get(slugify(col_name))
            if value is None or (isinstance(value, str) and not value.strip()):
                continue

            # Дополнительные проверки для специфических типов
            err_msg = None
            err_type = None
            str_val = str(value).strip()

            if col_type == 'money':
                cleaned = re.sub(r'[^\d,.-]', '', str_val)
                try:
                    if not cleaned: raise ValueError()
                    float(cleaned.replace(',', '.'))
                except ValueError:
                    err_msg, err_type = f"Значение '{value}' не является суммой", "INVALID_MONEY"
            
            elif col_type == 'date':
                if not self._validate_date_format(str_val, formats):
                    err_msg, err_type = f"Дата '{value}' не соответствует формату {formats}", "INVALID_DATE"
            
            elif col_type == 'time':
                if not re.match(self.TIME_PATTERN, str_val):
                    err_msg, err_type = f"Время '{value}' не соответствует формату HH:MM", "INVALID_TIME"

            if err_msg:
                errors.append(ValidationError(
                    row_index=row_index,
                    column=col_name,
                    value=value,
                    error_type=err_type,
                    message=err_msg
                ))

        return errors

    def validate_dataset(self, rows: List[Dict[str, Any]], entity_name: str) -> ValidationResult:
        """Валидирует весь набор данных."""
        contract = self.load_contract(entity_name)
        # Сохраняем имя сущности в контракте для _get_model_for_contract
        contract['name'] = entity_name
        
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

    def _validate_date_format(self, value: str, formats: Any) -> bool:
        """Проверяет соответствие даты одному из форматов."""
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
