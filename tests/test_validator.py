"""Тесты для валидатора контрактов."""
import pytest
from pathlib import Path
import sys

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.validator import ContractValidator, ValidationError, ValidationResult


class TestContractValidator:
    """Тесты для ContractValidator."""
    
    @pytest.fixture
    def validator(self):
        """Создает валидатор с путем к контрактам."""
        contracts_dir = Path(__file__).parent.parent / 'src' / 'contracts'
        return ContractValidator(contracts_dir)
    
    # === Тесты загрузки контрактов ===
    
    def test_load_existing_contract(self, validator):
        """Контракт успешно загружается."""
        contract = validator.load_contract('clients')
        assert 'entity' in contract
        assert contract['entity'] == 'Clients'
        assert 'columns' in contract
    
    def test_load_nonexistent_contract_raises(self, validator):
        """Несуществующий контракт выбрасывает ошибку."""
        with pytest.raises(FileNotFoundError):
            validator.load_contract('nonexistent_entity')
    
    def test_contract_caching(self, validator):
        """Контракт кешируется после первой загрузки."""
        contract1 = validator.load_contract('clients')
        contract2 = validator.load_contract('clients')
        assert contract1 is contract2
    
    # === Тесты валидации обязательных полей ===
    
    def test_missing_required_field_fails(self, validator):
        """Отсутствие обязательного поля — ошибка."""
        contract = validator.load_contract('sales')
        row = {'продукт': 'Абонемент', 'количество': '1'}  # нет 'дата' и 'клиент'
        
        errors = validator.validate_row(row, contract, 0)
        
        assert len(errors) >= 2
        error_columns = [e.column for e in errors]
        assert 'дата' in error_columns
        assert 'клиент' in error_columns
    
    def test_empty_string_required_field_fails(self, validator):
        """Пустая строка в обязательном поле — ошибка."""
        contract = validator.load_contract('sales')
        row = {'дата': '', 'клиент': '   ', 'продукт': 'Товар'}
        
        errors = validator.validate_row(row, contract, 0)
        
        error_columns = [e.column for e in errors]
        assert 'дата' in error_columns
        assert 'клиент' in error_columns
    
    def test_optional_field_can_be_empty(self, validator):
        """Необязательное поле может быть пустым."""
        contract = validator.load_contract('sales')
        row = {
            'дата': '01.01.25',
            'клиент': 'Иванов',
            'продукт': 'Товар',
            'комментарий': ''  # необязательное поле
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        # Не должно быть ошибок по 'комментарий'
        comment_errors = [e for e in errors if e.column == 'комментарий']
        assert len(comment_errors) == 0
    
    # === Тесты валидации типов данных ===
    
    def test_valid_integer(self, validator):
        """Корректное целое число проходит валидацию."""
        contract = validator.load_contract('sales')
        row = {
            'дата': '01.01.25',
            'клиент': 'Иванов',
            'продукт': 'Товар',
            'количество': '5'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        int_errors = [e for e in errors if e.column == 'количество']
        assert len(int_errors) == 0
    
    def test_invalid_integer_fails(self, validator):
        """Некорректное целое число — ошибка."""
        contract = validator.load_contract('sales')
        row = {
            'дата': '01.01.25',
            'клиент': 'Иванов',
            'продукт': 'Товар',
            'количество': 'пять'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        int_errors = [e for e in errors if e.column == 'количество']
        assert len(int_errors) == 1
        assert int_errors[0].error_type == 'INVALID_INTEGER'
    
    def test_valid_money_formats(self, validator):
        """Различные форматы денежных сумм проходят валидацию."""
        contract = validator.load_contract('sales')
        money_values = ['1000', '1 000', '1000.50', '1 000,50', '1000 руб', '1 500 р.']
        
        for money_val in money_values:
            row = {
                'дата': '01.01.25',
                'клиент': 'Иванов',
                'продукт': 'Товар',
                'полная_стоимость': money_val
            }
            errors = validator.validate_row(row, contract, 0)
            money_errors = [e for e in errors if e.column == 'полная_стоимость']
            assert len(money_errors) == 0, f"Неожиданная ошибка для значения: {money_val}"
    
    # === Тесты валидации дат ===
    
    def test_valid_date_formats(self, validator):
        """Различные форматы дат проходят валидацию."""
        contract = validator.load_contract('sales')
        valid_dates = ['01.12.25', '15.06.24', '31.12.99']
        
        for date_val in valid_dates:
            row = {
                'дата': date_val,
                'клиент': 'Иванов',
                'продукт': 'Товар'
            }
            errors = validator.validate_row(row, contract, 0)
            date_errors = [e for e in errors if e.column == 'дата']
            assert len(date_errors) == 0, f"Неожиданная ошибка для даты: {date_val}"
    
    def test_date_with_weekday_prefix(self, validator):
        """Дата с префиксом дня недели проходит валидацию."""
        contract = validator.load_contract('sales')
        row = {
            'дата': 'пн 01.12.25',
            'клиент': 'Иванов',
            'продукт': 'Товар'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        date_errors = [e for e in errors if e.column == 'дата']
        assert len(date_errors) == 0
    
    def test_invalid_date_fails(self, validator):
        """Некорректный формат даты — ошибка."""
        contract = validator.load_contract('sales')
        row = {
            'дата': '2025/12/01',  # Format forbidden in contract
            'клиент': 'Иванов',
            'продукт': 'Товар'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        date_errors = [e for e in errors if e.column == 'дата']
        assert len(date_errors) == 1
        assert date_errors[0].error_type == 'INVALID_DATE'
    
    # === Тесты валидации времени ===
    
    def test_valid_time_formats(self, validator):
        """Корректные форматы времени проходят валидацию."""
        contract = validator.load_contract('schedule')
        valid_times = ['09:00', '9:30', '12:00:00', '23:59']
        
        for time_val in valid_times:
            row = {
                'дата': '01.12.',
                'начало': time_val,
                'конец': '10:00',
                'клиент': 'Иванов',
                'статус': 'Занято'
            }
            errors = validator.validate_row(row, contract, 0)
            time_errors = [e for e in errors if e.column == 'начало']
            assert len(time_errors) == 0, f"Неожиданная ошибка для времени: {time_val}"
    
    def test_invalid_time_fails(self, validator):
        """Некорректный формат времени — ошибка."""
        contract = validator.load_contract('schedule')
        row = {
            'дата': '01.12.',
            'начало': 'утро',
            'конец': '10:00',
            'клиент': 'Иванов',
            'статус': 'Занято'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        time_errors = [e for e in errors if e.column == 'начало']
        assert len(time_errors) == 1
        assert time_errors[0].error_type == 'INVALID_TIME'
    
    # === Тесты валидации набора данных ===
    
    def test_validate_dataset_all_valid(self, validator):
        """Полностью валидный набор данных."""
        rows = [
            {'дата': '01.12.25', 'клиент': 'Иванов', 'продукт': 'Абонемент'},
            {'дата': '02.12.25', 'клиент': 'Петров', 'продукт': 'Разовое'},
        ]
        
        result = validator.validate_dataset(rows, 'sales')
        
        assert result.is_valid is True
        assert result.total_rows == 2
        assert result.valid_rows == 2
        assert result.error_rate == 0.0
    
    def test_validate_dataset_partial_valid(self, validator):
        """Частично валидный набор данных."""
        rows = [
            {'дата': '01.12.25', 'клиент': 'Иванов', 'продукт': 'Абонемент'},
            {'дата': '', 'клиент': 'Петров', 'продукт': 'Разовое'},  # невалидная
            {'дата': '03.12.25', 'клиент': 'Сидоров', 'продукт': 'Товар'},
        ]
        
        result = validator.validate_dataset(rows, 'sales')
        
        assert result.is_valid is False
        assert result.total_rows == 3
        assert result.valid_rows == 2
        assert len(result.errors) >= 1
    
    def test_validate_empty_dataset(self, validator):
        """Пустой набор данных считается валидным."""
        result = validator.validate_dataset([], 'sales')
        
        assert result.is_valid is True
        assert result.total_rows == 0
        assert result.valid_rows == 0


class TestValidationResult:
    """Тесты для ValidationResult."""
    
    def test_error_rate_calculation(self):
        """Расчет процента ошибок."""
        result = ValidationResult(
            is_valid=False,
            total_rows=100,
            valid_rows=75,
            errors=[]
        )
        
        assert result.error_rate == 0.25
    
    def test_error_rate_zero_rows(self):
        """Процент ошибок для пустого набора."""
        result = ValidationResult(
            is_valid=True,
            total_rows=0,
            valid_rows=0,
            errors=[]
        )
        
        assert result.error_rate == 0.0


class TestEdgeCases:
    """Тесты граничных случаев."""
    
    @pytest.fixture
    def validator(self):
        contracts_dir = Path(__file__).parent.parent / 'src' / 'contracts'
        return ContractValidator(contracts_dir)
    
    def test_none_value_in_row(self, validator):
        """None значения обрабатываются корректно."""
        contract = validator.load_contract('sales')
        row = {
            'дата': '01.12.25',
            'клиент': None,  # None в обязательном поле
            'продукт': 'Товар'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        assert any(e.column == 'клиент' for e in errors)
    
    def test_numeric_value_as_string(self, validator):
        """Числовые значения как строки обрабатываются."""
        contract = validator.load_contract('sales')
        row = {
            'дата': '01.12.25',
            'клиент': 'Иванов',
            'продукт': 'Товар',
            'количество': 5  # int вместо str
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        int_errors = [e for e in errors if e.column == 'количество']
        assert len(int_errors) == 0  # Должно сконвертироваться в строку
    
    def test_whitespace_only_value(self, validator):
        """Значение только из пробелов считается пустым."""
        contract = validator.load_contract('sales')
        row = {
            'дата': '01.12.25',
            'клиент': '   \t\n   ',  # только whitespace
            'продукт': 'Товар'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        assert any(e.column == 'клиент' and e.error_type == 'MISSING_REQUIRED' for e in errors)
    
    def test_negative_money_value(self, validator):
        """Отрицательные денежные суммы валидны."""
        contract = validator.load_contract('sales')
        row = {
            'дата': '01.12.25',
            'клиент': 'Иванов',
            'продукт': 'Возврат',
            'полная_стоимость': '-1000'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        money_errors = [e for e in errors if e.column == 'полная_стоимость']
        assert len(money_errors) == 0

    def test_slugified_keys_validation(self, validator):
        """Валидатор корректно обрабатывает транслитерированные (slugified) ключи."""
        contract = validator.load_contract('sales')
        # Имитируем данные от Extractor'а, где ключи уже транслитерированы
        # 'дата' -> 'data', 'клиент' -> 'klient', 'продукт' -> 'produkt'
        row = {
            'data': '01.12.25',
            'klient': 'Иванов',
            'produkt': 'Товар',
            'kolichestvo': '5'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        # Ошибок быть не должно, т.к. валидатор должен найти 'data' для 'дата'
        assert len(errors) == 0, f"Errors found with slugified keys: {errors}"

    def test_complex_header_mapping(self, validator):
        """Проверка сложного маппинга заголовков (спецсимволы, регистр, пробелы)."""
        # Создаем временный контракт "на лету"
        custom_contract = {
            "entity": "TestEntity",
            "columns": [
                {"name": "Полная Стоимость", "type": "string", "required": True},  # -> polnaya_stoimost
                {"name": "Email Адрес", "type": "string"},  # -> email_adres
                {"name": "№ Телефона", "type": "string"}, # -> telefon (если спецсимволы убираются)
            ]
        }
        
        # Симулируем данные от Extractor (который прогнал slugify)
        row = {
            'polnaya_stoimost': '100',
            'email_adres': 'test@example.com',
            'telefon': '123'
        }
        
        errors = validator.validate_row(row, custom_contract, 0)
        assert len(errors) == 0, f"Validator failed to map complex headers: {[e.column for e in errors]}"
        
        # Проверяем, что отсутствие slugified ключа все равно вызывает ошибку
        bad_row = {
            'wrong_column': '100',
            'email_adres': 'test@example.com'
        }
        errors_bad = validator.validate_row(bad_row, custom_contract, 0)
        assert any(e.column == 'Полная Стоимость' and e.error_type == 'MISSING_REQUIRED' for e in errors_bad)

    def test_column_renaming_fails_validation(self, validator):
        """Смена названия обязательной колонки в источнике должна вызывать ошибку."""
        contract = validator.load_contract('sales')
        # Ожидаем: 'дата', 'клиент'
        # Пришло: 'дата', 'покупатель' (переименовали 'клиент' -> 'покупатель')
        row = {
            'data': '01.12.25',
            'pokupatel': 'Иванов', # slugify('покупатель')
            'produkt': 'Товар'
        }
        
        errors = validator.validate_row(row, contract, 0)
        
        # Должны получить ошибку, что нет поля 'клиент'
        client_errors = [e for e in errors if e.column == 'клиент']
        assert len(client_errors) > 0
        assert client_errors[0].error_type == 'MISSING_REQUIRED'
        
        # 'покупатель' просто игнорируется (валидатор проверяет только то, что в контракте)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
