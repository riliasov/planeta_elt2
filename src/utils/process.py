import os
import sys
import signal
import logging
from pathlib import Path

log = logging.getLogger('process')

class ProcessLock:
    """Управление блокировкой процесса через PID-файл."""
    
    def __init__(self, name: str = "elt_pipeline"):
        self.lock_dir = Path("logs")
        self.lock_dir.mkdir(exist_ok=True)
        self.lock_file = self.lock_dir / f"{name}.pid"
        self.name = name

    def check_and_lock(self, kill_conflicts: bool = False, timeout: int = 0):
        """Проверяет наличие lock-файла и создает его.
        
        Args:
            kill_conflicts: Если True, убивает процесс, удерживающий лок.
            timeout: Время ожидания освобождения лока в секундах.
        """
        import time
        start_time = time.time()
        
        while True:
            if self.lock_file.exists():
                try:
                    with open(self.lock_file, "r") as f:
                        old_pid = int(f.read().strip())
                    
                    if self._is_running(old_pid):
                        if kill_conflicts:
                            log.warning(f"Обнаружен конфликт: процесс {old_pid} уже запущен. Завершаю его...")
                            os.kill(old_pid, signal.SIGTERM)
                            time.sleep(1)
                            if self._is_running(old_pid):
                                log.warning(f"Процесс {old_pid} не завершился по SIGTERM. Принудительно завершаю (SIGKILL)...")
                                os.kill(old_pid, signal.SIGKILL)
                            # Лок должен удалиться или мы перезапишем его, если процесс убит
                        elif timeout > 0:
                            if time.time() - start_time > timeout:
                                log.error(f"Таблицы заблокированы процессом {old_pid}. Превышено время ожидания ({timeout}с).")
                                sys.exit(1)
                            log.info(f"Ожидание завершения процесса {old_pid}... ({int(time.time() - start_time)}/{timeout}с)")
                            time.sleep(1)
                            continue
                        else:
                            log.error(f"Критическая ошибка: Процесс '{self.name}' уже запущен (PID: {old_pid}).")
                            log.error("Используйте --wait [sec] для ожидания или --kill-conflicts для перезапуска.")
                            sys.exit(1)
                    else:
                        # Процесс мертв, но файл остался (stale lock)
                        log.warning(f"Найден устаревший lock-файл от PID {old_pid} (процесс не найден).")
                except (ValueError, OSError):
                    # Файл битый
                    pass
                
                # Если пришли сюда - лок можно удалять/перезаписывать (либо убили, либо старый, либо битый)
                try:
                    self.lock_file.unlink()
                except OSError:
                    pass

            # Пытаемся создать лок
            try:
                # Используем 'x' mode для атомарного создания, но это не гарантирует атомарность на всех ФС.
                # Для простоты: пишем и проверяем.
                with open(self.lock_file, "w") as f:
                    f.write(str(os.getpid()))
                log.debug(f"Создан lock-файл: {self.lock_file} (PID: {os.getpid()})")
                break
            except OSError as e:
                # Гонка?
                if timeout > 0 and (time.time() - start_time < timeout):
                    time.sleep(0.5)
                    continue
                log.error(f"Не удалось создать lock-файл: {e}")
                sys.exit(1)

    def unlock(self):
        """Удаляет lock-файл."""
        try:
            if self.lock_file.exists():
                self.lock_file.unlink()
                log.debug(f"Lock-файл удален: {self.lock_file}")
        except OSError as e:
            log.error(f"Ошибка при удалении lock-файла: {e}")

    def _is_running(self, pid: int) -> bool:
        """Проверяет запущен ли процесс с данным PID."""
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
