import logging
import sys
from typing import Optional

def setup_logger(name: str = "elt", level: str = "INFO") -> logging.Logger:
    """Настраивает и возвращает логгер с заданным именем и уровнем."""
    logger = logging.getLogger(name)
    
    # Если логгер уже настроен, просто возвращаем его
    if logger.handlers:
        return logger
        
    logger.setLevel(level)
    
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt='%(levelname)s | %(name)s | %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger
