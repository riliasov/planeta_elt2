import sys
import os
import time
import multiprocessing

# Добавляем корень проекта в путь
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.process import ProcessLock

def run_locked_process(name, duration, results_queue):
    lock = ProcessLock("test_lock")
    try:
        print(f"[{name}] Пытаюсь взять лок...")
        lock.check_and_lock(timeout=0)
        print(f"[{name}] Лок получен!")
        results_queue.put((name, True))
        time.sleep(duration)
        lock.unlock()
        print(f"[{name}] Лок освобожден.")
    except SystemExit:
        print(f"[{name}] Не удалось получить лок (ожидаемо).")
        results_queue.put((name, False))
    except Exception as e:
        print(f"[{name}] Ошибка: {e}")
        results_queue.put((name, False))

if __name__ == "__main__":
    results = multiprocessing.Queue()
    
    # Запускаем первый процесс
    p1 = multiprocessing.Process(target=run_locked_process, args=("Proc1", 3, results))
    p1.start()
    
    # Ждем немного, чтобы первый точно взял лок
    time.sleep(1)
    
    # Запускаем второй процесс
    p2 = multiprocessing.Process(target=run_locked_process, args=("Proc2", 1, results))
    p2.start()
    
    p1.join()
    p2.join()
    
    # Проверяем результаты
    outcomes = {}
    while not results.empty():
        name, success = results.get()
        outcomes[name] = success
    
    if outcomes.get("Proc1") is True and outcomes.get("Proc2") is False:
        print("\n✅ Тест пройден: Блокировка работает корректно!")
        sys.exit(0)
    else:
        print(f"\n❌ Тест провален: {outcomes}")
        sys.exit(1)
