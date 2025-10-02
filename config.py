import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    API_ID = os.getenv('TELEGRAM_API_ID')
    API_HASH = os.getenv('TELEGRAM_API_HASH')
    PHONE_NUMBER = os.getenv('TELEGRAM_PHONE')
    CHANNEL_NAME = os.getenv('CHANNEL_NAME')
    
    @classmethod
    def validate(cls):
        required_fields = ['API_ID', 'API_HASH', 'PHONE_NUMBER', 'CHANNEL_NAME']
        missing_fields = []
        
        for field in required_fields:
            if not getattr(cls, field):
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Отсутствуют обязательные настройки: {', '.join(missing_fields)}")
        
        return True
