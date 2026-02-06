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
    
    # --- AI Params ---
    AI_TOP_N = os.getenv('AI_TOP_N')                # Number of best price levels to be transmitted accurately
    AI_TAIL_BINS = os.getenv('AI_TAIL_BINS')        # Number of "bins" for distant levels.
    AI_TAIL_MAX_BPS = os.getenv('AI_TAIL_MAX_BPS')  # The tail covers 5% of the price movement
    
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
