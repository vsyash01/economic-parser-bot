import sqlite3
import logging
from pathlib import Path
from datetime import date, datetime
from typing import Optional, Dict
from config import Config

logger = logging.getLogger(__name__)

class NewsDatabase:
    def __init__(self, db_path: str = None):
        self.db_path = Path(db_path) if db_path else Path(Config.DB_PATH)
        self._init_db()  # Инициализация БД при создании экземпляра

    def _init_db(self):
        """Полная инициализация структуры БД"""
        # Создаем директорию для БД, если её нет
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # Таблица новостей
            conn.execute('''
                CREATE TABLE IF NOT EXISTS news (
                    id TEXT PRIMARY KEY,
                    source TEXT,
                    title TEXT,
                    url TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
            # Таблица закрепленных сообщений
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pinned_messages (
                    date TEXT PRIMARY KEY,
                    message_id INTEGER,
                    last_updated TEXT
                )
            ''')
        
            # Таблица разделов закрепленных сообщений
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pinned_sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    section_type TEXT,
                    content TEXT,
                    update_time TEXT,
                    UNIQUE(date, section_type)
                )
            ''')
        
            # Индексы для ускорения запросов
            conn.execute('CREATE INDEX IF NOT EXISTS idx_news_id ON news (id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_news_timestamp ON news (timestamp)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_pinned_date ON pinned_messages (date)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_sections_date ON pinned_sections (date)')

    def get_pinned_message_id(self) -> Optional[int]:
        """Получает ID закрепленного сообщения за сегодня"""
        today = date.today().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT message_id FROM pinned_messages WHERE date = ?',
                (today,)
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def save_pinned_message(self, message_id: int):
        """Сохраняет или обновляет закрепленное сообщение"""
        today = date.today().isoformat()
        now_str = datetime.now().strftime('%H:%M')
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO pinned_messages (date, message_id, last_updated) '
                'VALUES (?, ?, ?)',
                (today, message_id, now_str)
            )

    def get_pinned_sections(self) -> Dict[str, Dict]:
        """Получает все сохраненные разделы за сегодня"""
        today = date.today().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT section_type, content, update_time '
                'FROM pinned_sections WHERE date = ?',
                (today,)
            )
            
            sections = {}
            for row in cursor.fetchall():
                sections[row[0]] = {
                    'content': row[1],
                    'time': row[2],
                    'emoji': self._get_section_emoji(row[0])
                }
            return sections

    def save_pinned_section(self, section_type: str, content: str, update_time: str):
        """Сохраняет или обновляет раздел"""
        today = date.today().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO pinned_sections '
                '(date, section_type, content, update_time) '
                'VALUES (?, ?, ?, ?)',
                (today, section_type, content, update_time)
            )

    @staticmethod
    def _get_section_emoji(section_type: str) -> str:
        """Возвращает эмодзи для раздела"""
        emojis = {
            'stocks': '📊',
            'dividends': '💵',
            'commodities': '🛢️',
            'crypto': '💰'
        }
        return emojis.get(section_type, '📌')

    async def cleanup_old_pins(self):
        """Очистка старых закреплений"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'DELETE FROM pinned_messages WHERE date < date("now", "-3 days")'
            )
            logger.info("Cleaned up old pinned messages")

    async def is_news_exists(self, news_id: str) -> bool:
        """Проверка существования новости"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM news WHERE id = ?', (news_id,))
            return cursor.fetchone() is not None

    async def add_news(self, news_id: str, source: str, title: str, url: str):
        """Добавление новости в БД"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR IGNORE INTO news (id, source, title, url)
                VALUES (?, ?, ?, ?)
            ''', (news_id, source, title, url))
            logger.debug(f"Added news: {title[:50]}...")

    async def cleanup_old_news(self):
        """Очистка старых записей"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'DELETE FROM news WHERE timestamp < datetime("now", ?)',
                (f"-{Config.DB_CLEANUP_DAYS} days",)
            )
            logger.info(f"Cleaned up {cursor.rowcount} old records")