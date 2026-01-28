# =============================
# file: config.py
# =============================

import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    API_ID = os.getenv('TELEGRAM_API_ID')
    API_HASH = os.getenv('TELEGRAM_API_HASH')
    TELEGRAM_PHONE = os.getenv('TELEGRAM_PHONE')
    CHANNEL_NAME = os.getenv('CHANNEL_NAME')
    TTL_SECONDS = float(os.getenv('TTL_SECONDS'))
    AGG_INTERVAL_SEC = float(os.getenv('AGG_INTERVAL_SEC'))
    
    PROFILING = True
    
    @classmethod
    def validate(cls):
        required_fields = ['API_ID', 'API_HASH', 'TELEGRAM_PHONE', 'CHANNEL_NAME', 'TTL_SECONDS', 'AGG_INTERVAL_SEC']
        missing_fields = []
        
        for field in required_fields:
            if not getattr(cls, field):
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Отсутствуют обязательные настройки: {', '.join(missing_fields)}")
        
        return True
