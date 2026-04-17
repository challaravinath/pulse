"""Configuration utilities"""
from dotenv import load_dotenv
load_dotenv()

import os
from dataclasses import dataclass

@dataclass
class AppConfig:
    cache_dir: str = "./cache"
    log_level: str = "INFO"
    
    @classmethod
    def load(cls):
        return cls(
            cache_dir=os.getenv("CACHE_DIR", "./cache"),
            log_level=os.getenv("PULSE_LOG_LEVEL", "INFO")
        )
