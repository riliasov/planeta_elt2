import logging
import sys
from typing import Optional

def setup_logger(name: str = None, level: str = "INFO") -> logging.Logger:
    """Настраивает базовое логирование (в том числе для root)."""
    # Если name не передан, настраиваем корневой логгер
    logger = logging.getLogger(name)
    
    # Если у логгера уже есть хендлеры, не добавляем дубликаты
    if logger.handlers:
        return logger
        
    logger.setLevel(level)
    
    # Форматтер
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 1. Console Handler
    c_handler = logging.StreamHandler(sys.stdout)
    c_handler.setFormatter(formatter)
    logger.addHandler(c_handler)
    
    # 2. File Handler (Rotating)
    try:
        from logging.handlers import RotatingFileHandler
        import os
        from pathlib import Path
        
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
            
        f_handler = RotatingFileHandler(
            filename=log_dir / "elt.log",
            maxBytes=5*1024*1024,  # 5 MB
            backupCount=3,
            encoding='utf-8'
        )
        f_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
    except Exception as e:
        print(f"Failed to setup file logging: {e}", file=sys.stderr)
    
    return logger

