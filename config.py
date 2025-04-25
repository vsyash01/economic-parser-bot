import os
from pathlib import Path
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv('keys.env')

class Config:
    # Telegram
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    TARGET_CHANNEL_ID = os.getenv('TARGET_CHANNEL_ID')
    # ID канала для новостей (может отличаться от основного)
    NEWS_CHANNEL_ID = os.getenv('TARGET_CHANNEL_ID')  # Или отдельный ID
    
    # База данных
    RECREATE_DB = False
    DB_PATH = "data/economic_parser.db"  # Изменил путь для лучшей организации
    DB_CLEANUP_DAYS = 30
    
    # Интервалы
    UPDATE_INTERVAL = int(os.getenv('UPDATE_INTERVAL', 1800))  # 30 мин
    
    # Yandex Cloud
    YANDEX_FUNCTION_ID = os.getenv('YANDEX_FUNCTION_ID')
    YANDEX_FOLDER_ID = os.getenv('YANDEX_FOLDER_ID')
    
    # Логирование
    LOGGING = {
        'level': 'INFO',
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    }
    
    @classmethod
    def check_required(cls):
        """Проверка обязательных настроек"""
        # Для диагностики добавьте в config.py перед проверкой:
        #print("Current working directory:", os.getcwd())
        #print("Trying to load .env from:", 'keys.env')
        required = [
            cls.TELEGRAM_TOKEN, 
            cls.TELEGRAM_CHAT_ID,
            cls.TARGET_CHANNEL_ID
        ]
        if not all(required):
            missing = [name for name, val in zip(
                ['TELEGRAM_TOKEN', 'TELEGRAM_CHAT_ID', 'TARGET_CHANNEL_ID'],
                required
            ) if not val]
            raise ValueError(f"Missing required configs: {', '.join(missing)}")

# Проверка при импорте
Config.check_required()