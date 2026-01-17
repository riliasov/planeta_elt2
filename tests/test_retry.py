"""Тесты для модуля retry (синхронные тесты)."""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from src.utils.retry import (
    retry_async,
    with_retry,
    RetryError,
    is_rate_limit_error
)


@pytest.mark.asyncio
class TestRetryAsync:
    async def test_success_first_attempt(self):
        mock_func = AsyncMock(return_value='success')
        result = await retry_async(mock_func, max_attempts=3)
        assert result == 'success'
        assert mock_func.call_count == 1
    
    async def test_success_after_retry(self):
        mock_func = AsyncMock(side_effect=[Exception('fail'), 'success'])
        result = await retry_async(mock_func, max_attempts=3, base_delay=0.01)
        assert result == 'success'
        assert mock_func.call_count == 2
    
    async def test_raises_after_max_attempts(self):
        mock_func = AsyncMock(side_effect=Exception('always fails'))
        try:
            await retry_async(mock_func, max_attempts=2, base_delay=0.01)
            assert False, "Should have raised RetryError"
        except RetryError as e:
            assert 'Failed after 2 attempts' in str(e)
        assert mock_func.call_count == 2
    
    async def test_on_retry_callback(self):
        mock_func = AsyncMock(side_effect=[Exception('fail'), 'success'])
        callback = MagicMock()
        
        await retry_async(
            mock_func, 
            max_attempts=3, 
            base_delay=0.01,
            on_retry=callback
        )
        
        assert callback.call_count == 1


@pytest.mark.asyncio
class TestWithRetryDecorator:
    async def test_decorator_success(self):
        @with_retry(max_attempts=2, base_delay=0.01)
        async def my_func():
            return 'ok'
        
        result = await my_func()
        assert result == 'ok'
    
    async def test_decorator_retry_on_failure(self):
        attempts = []
        
        @with_retry(max_attempts=3, base_delay=0.01)
        async def my_func():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError('not yet')
            return 'done'
        
        result = await my_func()
        assert result == 'done'
        assert len(attempts) == 2


class TestIsRateLimitError:
    def test_429_error(self):
        assert is_rate_limit_error(Exception('429 Too Many Requests')) is True
    
    def test_rate_limit_message(self):
        assert is_rate_limit_error(Exception('Rate limit exceeded')) is True
    
    def test_quota_exceeded(self):
        assert is_rate_limit_error(Exception('Quota exceeded for the day')) is True
    
    def test_regular_error(self):
        assert is_rate_limit_error(Exception('Connection refused')) is False
    
    def test_resource_exhausted(self):
        assert is_rate_limit_error(Exception('Resource exhausted')) is True
