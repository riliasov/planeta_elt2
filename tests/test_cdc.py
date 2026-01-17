import sys
import os

# Добавляем корень проекта в путь
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.etl.cdc_processor import CDCProcessor

def test_cdc_processor_logic():
    # Исходное состояние: {pk: hash}
    existing = {
        "1": "hash_old",
        "2": "hash_stable"
    }
    processor = CDCProcessor(existing)
    
    # Случай 1: Новая строка
    processor.process_row(pk="3", row_hash="hash_new", row_data={"id": "3", "val": "A"})
    
    # Случай 2: Измененная строка
    processor.process_row(pk="1", row_hash="hash_updated", row_data={"id": "1", "val": "B"})
    
    # Случай 3: Неизмененная строка
    processor.process_row(pk="2", row_hash="hash_stable", row_data={"id": "2", "val": "C"})
    
    # Завершаем (должна найтись удаленная строка, но в этом тесте их нет в начале, 
    # кроме тех, что мы НЕ обработали. Но мы обработали все 1 и 2. 
    # Чтобы протестировать удаление, добавим в existing еще одну.)
    
    processor.finalize()
    stats = processor.get_stats()
    
    print(f"Stats: {stats}")
    assert stats['inserted'] == 1
    assert stats['updated'] == 1
    assert stats['unchanged'] == 1
    assert stats['deleted'] == 0 # Мы обработали все исходные
    
    # Проверка удаления
    existing_with_del = {"99": "some_hash"}
    proc_del = CDCProcessor(existing_with_del)
    proc_del.finalize()
    assert proc_del.get_stats()['deleted'] == 1
    assert proc_del.to_delete == ["99"]

if __name__ == "__main__":
    try:
        test_cdc_processor_logic()
        print("✅ CDC Processor logic verified!")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)
