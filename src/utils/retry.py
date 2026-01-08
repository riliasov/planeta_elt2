"""Утилиты для retry с exponential backoff."""
import asyncio
import logging
import random
from typing import TypeVar, Callable, Any
from functools import wraps
from src.config.constants import RETRY_MAX_ATTEMPTS, RETRY_BASE_DELAY

log = logging.getLogger('retry')

T = TypeVar('T')


class RetryError(Exception):
    """Исключение после исчерпания попыток."""
    def __init__(self, message: str, last_error: Exception = None):
        super().__init__(message)
        self.last_error = last_error


async def retry_async(
    func: Callable[..., T],
    *args,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,),
    on_retry: Callable[[int, Exception], None] = None,
    **kwargs
) -> T:
    """Выполняет async функцию с exponential backoff retry."""
    last_exception = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            
            if attempt == max_attempts:
                log.error(f"Retry exhausted after {max_attempts} attempts: {e}")
                raise RetryError(
                    f"Failed after {max_attempts} attempts", 
                    last_error=e
                ) from e
            
            # Exponential backoff с jitter
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            jitter = random.uniform(0, delay * 0.1)
            sleep_time = delay + jitter
            
            log.warning(f"Attempt {attempt}/{max_attempts} failed: {e}. Retrying in {sleep_time:.1f}s...")
            
            if on_retry:
                on_retry(attempt, e)
            
            await asyncio.sleep(sleep_time)
    
    # Этот код не должен выполняться, но на всякий случай
    raise RetryError("Unexpected retry loop exit", last_error=last_exception)


def with_retry(
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
    exceptions: tuple = (Exception,)
):
    """Декоратор для async функций с retry."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            return await retry_async(
                func, *args,
                max_attempts=max_attempts,
                base_delay=base_delay,
                exceptions=exceptions,
                **kwargs
            )
        return wrapper
    return decorator


def is_rate_limit_error(error: Exception) -> bool:
    """Проверяет, является ли ошибка rate limit от Google API."""
    error_str = str(error).lower()
    return any(indicator in error_str for indicator in [
        '429', 'rate limit', 'quota exceeded', 
        'too many requests', 'resource exhausted'
    ])
