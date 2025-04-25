import asyncio
import logging
from pathlib import Path
import sqlite3
from pathlib import Path
from datetime import datetime
from aiogram import Dispatcher
from config import Config
from services.telegram_client import TelegramClient
from parsers.moex import MOEXParser
from parsers.dividends import DividendsParser
from parsers.news_ru import RussianNewsParser
from parsers.tradingeconomics import TradingEconomicsParser
from database import NewsDatabase
from parsers.company_reports import CompanyReportsParser

# Настройка логгирования
logging.basicConfig(
    level=Config.LOGGING['level'],
    format=Config.LOGGING['format']
)
logger = logging.getLogger(__name__)

async def check_database(db: NewsDatabase):
    """Проверяет и восстанавливает структуру БД при необходимости"""
    try:
        # Простая проверка существования таблиц
        with sqlite3.connect(Config.DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
            
            required_tables = {'news', 'pinned_messages', 'pinned_sections'}
            if not required_tables.issubset(tables):
                logger.warning("Database tables missing, recreating...")
                db._init_db()  # Пересоздаем структуру
                logger.info("Database reinitialized successfully")
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        raise

class EconomicParserBot:
    def __init__(self):
        # Удаление старой БД при необходимости
        if Config.RECREATE_DB and Path(Config.DB_PATH).exists():
            Path(Config.DB_PATH).unlink()
        
        # Инициализация БД (теперь db_path будет установлен автоматически)
        self.db = NewsDatabase()
        
        # Проверка и восстановление БД
        asyncio.run(check_database(self.db))
        
        # Инициализация Dispatcher и клиента Telegram
        self.dp = Dispatcher()
        self.tg = TelegramClient(self.dp, self.db)
        
        # Инициализация парсеров
        self.parsers = [
            MOEXParser(self.dp, self.db),
            DividendsParser(self.dp, self.db),
            RussianNewsParser(self.dp, self.db),
            TradingEconomicsParser(self.dp, self.db),
            CompanyReportsParser(self.dp, self.db)
        ]

    async def run_parsing(self):
        """Запуск всех парсеров"""
        logger.info("Starting parsing cycle")
        
        # Очистка старых новостей в полночь
        now = datetime.now()
        if now.hour == 0 and now.minute < 5:
            await self.db.cleanup_old_news()
        
        results = await asyncio.gather(
            *[parser.parse() for parser in self.parsers],
            return_exceptions=True
        )
        
        for parser, result in zip(self.parsers, results):
            if isinstance(result, Exception):
                logger.error(f"Parser {parser.__class__.__name__} failed: {result}")
        
        logger.info("Parsing cycle completed")
        return all(not isinstance(r, Exception) for r in results)

    async def periodic_update(self):
        """Периодический запуск парсинга"""
        while True:
            try:
                await self.run_parsing()
            except Exception as e:
                logger.error(f"Parsing failed: {e}", exc_info=True)
            await asyncio.sleep(Config.UPDATE_INTERVAL)

    async def start(self):
        """Запуск бота"""
        try:
            await self.tg.bot.delete_webhook(drop_pending_updates=True)
            asyncio.create_task(self.periodic_update())
            await self.dp.start_polling(self.tg.bot)
        except Exception as e:
            logger.critical(f"Bot failed: {e}")
            raise

if __name__ == "__main__":
    # Удаление старой БД при необходимости (для тестов)
    if Config.RECREATE_DB and Path(Config.DB_PATH).exists():
        Path(Config.DB_PATH).unlink()
        logger.info("Old database removed")

    bot = EconomicParserBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Critical error: {e}")