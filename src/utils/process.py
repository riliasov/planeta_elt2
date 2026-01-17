import os
import sys
import signal
import logging
import time
import fcntl
from pathlib import Path

log = logging.getLogger('process')

class ProcessLock:
    """Управление блокировкой процесса через POSIX flock."""
    
    def __init__(self, name: str = "elt_pipeline"):
        self.lock_dir = Path("logs")
        self.lock_dir.mkdir(exist_ok=True)
        self.lock_file = self.lock_dir / f"{name}.lock"
        self.name = name
        self.lock_fd = None

    def check_and_lock(self, kill_conflicts: bool = False, timeout: int = 0):
        """Проверяет наличие лока и устанавливает его через fcntl.flock.
        
        Args:
            kill_conflicts: Если True, пытается убить процесс, удерживающий лок.
            timeout: Время ожидания освобождения лока в секундах.
        """
        start_time = time.time()
        
        while True:
            # Открываем файл для чтения/записи (создаем если нет)
            self.lock_fd = open(self.lock_file, "a+")
            
            try:
                # Пытаемся взять эксклюзивный неблокирующий лок
                fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Лок взят успешно!
                # Очищаем файл и записываем текущий PID
                self.lock_fd.seek(0)
                self.lock_fd.truncate()
                self.lock_fd.write(str(os.getpid()))
                self.lock_fd.flush()
                log.debug(f"Лок '{self.name}' получен успешно (PID: {os.getpid()})")
                return
                
            except BlockingIOError:
                # Файл заблокирован другим процессом
                self.lock_fd.seek(0)
                try:
                    old_pid_str = self.lock_fd.read().strip()
                    old_pid = int(old_pid_str) if old_pid_str else None
                except ValueError:
                    old_pid = None
                
                if kill_conflicts and old_pid:
                    log.warning(f"Конфликт лока '{self.name}': процесс {old_pid} уже запущен. Пытаюсь завершить...")
                    try:
                        os.kill(old_pid, signal.SIGTERM)
                        time.sleep(1)
                        if self._is_running(old_pid):
                            log.warning(f"Процесс {old_pid} не сдается. Применяю SIGKILL...")
                            os.kill(old_pid, signal.SIGKILL)
                            time.sleep(0.5)
                    except (ProcessLookupError, OSError):
                        pass # Процесс уже мертв
                
                elif timeout > 0:
                    elapsed = time.time() - start_time
                    if elapsed > timeout:
                        log.error(f"Превышено время ожидания лока '{self.name}' ({timeout}с).")
                        sys.exit(1)
                    
                    log.info(f"Ожидание освобождения лока '{self.name}' (PID: {old_pid or '??'})... {int(elapsed)}/{timeout}с")
                    self.lock_fd.close()
                    time.sleep(1)
                    continue
                else:
                    log.error(f"Критическая ошибка: Пайплайн '{self.name}' уже запущен (PID: {old_pid or '??'}).")
                    log.error("Используйте --wait [sec] или --kill-conflicts.")
                    sys.exit(1)

    def unlock(self):
        """Освобождает лок и закрывает файл."""
        if self.lock_fd:
            try:
                # fcntl.flock(self.lock_fd, fcntl.LOCK_UN) # Разблокировка произойдет автоматически при close
                self.lock_fd.close()
                if self.lock_file.exists():
                    try:
                        self.lock_file.unlink()
                    except OSError:
                        pass
                log.debug(f"Лок '{self.name}' освобожден.")
            except Exception as e:
                log.error(f"Ошибка при освобождении лока: {e}")
            finally:
                self.lock_fd = None

    def _is_running(self, pid: int) -> bool:
        """Проверяет запущен ли процесс с данным PID."""
        if pid <= 0: return False
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False
