from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
    CallbackQuery,
    Message
)
from pathlib import Path
from config import Config
import logging
import re
from datetime import datetime, date
import asyncio
from database import NewsDatabase

logger = logging.getLogger(__name__)

class TelegramClient:
    def __init__(self, dp: Dispatcher, db: NewsDatabase):
        self.bot = Bot(token=Config.TELEGRAM_TOKEN)
        self._dp = dp
        self.db = db
        
        # Загружаем сохраненное состояние
        self.pinned_message_id = db.get_pinned_message_id()
        self.sections = db.get_pinned_sections() or {
            'stocks': {'content': None, 'time': None, 'emoji': '📊'},
            'dividends': {'content': None, 'time': None, 'emoji': '💵'},
            'commodities': {'content': None, 'time': None, 'emoji': '🛢️'},
            'crypto': {'content': None, 'time': None, 'emoji': '💰'}
        }
        
        self.current_date = date.today()
        self._register_handlers()
        logger.info(f"Initialized with pinned message: {self.pinned_message_id}")

    def _extract_content(self, message: Message) -> str:
        """Извлекаем контент, сохраняя ВСЕ эмодзи-индикаторы"""
        return message.caption if message.caption else message.text or ""
        text = message.caption if message.caption else message.text
        if not text:
            return ""
    
        # Удаляем только заголовок (первую строку)
        #content = text.split('\n', 1)[1].strip() if '\n' in text else text
        content = text.strip()  # убирает whitespace по краям, но сохраняет всё содержимое
    
        # Сохраняем все эмодзи в каждой строке
        lines = []
        indicator_emojis = {'🟢', '🔴', '⚪', '💰', '🛢️', '📊', '💵'}  # Все возможные индикаторы
    
        for line in content.split('\n'):
            if not line.strip():
                continue
            
            # Разделяем эмодзи и основной текст
            preserved_emojis = []
            rest_of_line = []
        
            for char in line:
                if char in indicator_emojis:
                    preserved_emojis.append(char)
                else:
                    # Прерываем после первого не-эмодзи символа (чтобы не сохранять эмодзи внутри текста)
                    if not rest_of_line and not char.isspace():
                        rest_of_line.append(char)
                    elif rest_of_line:
                        rest_of_line.append(char)
        
            # Собираем строку с сохраненными индикаторами
            cleaned_line = (' '.join(preserved_emojis) + ' ' + ''.join(rest_of_line)).strip()
            lines.append(cleaned_line)
    
        return '\n'.join(lines)

    def _register_handlers(self):
        """Регистрация обработчиков с защитой от ошибок"""
        @self._dp.callback_query(F.data == 'delete_message')
        async def delete_handler(callback: CallbackQuery):
            try:
                await callback.message.delete()
                await callback.answer("Сообщение удалено")
            except Exception as e:
                logger.error(f"Delete error: {e}")
                await callback.answer("Ошибка при удалении", show_alert=True)

        @self._dp.callback_query(F.data.startswith('update_pinned_'))
        async def update_pinned_handler(callback: CallbackQuery):
            try:
                content_type = callback.data.split('_')[-1]
                if content_type not in self.sections:
                    logger.error(f"Unknown content type: {content_type}")
                    await callback.answer("Неизвестный тип контента", show_alert=True)
                    return

                await self._process_pinned_update(callback, content_type)
                await callback.answer("Закреп обновлён")
            except Exception as e:
                logger.error(f"Pin update error: {e}", exc_info=True)
                await callback.answer("Ошибка при обновлении", show_alert=True)
                
        @self._dp.callback_query(F.data == 'forward_to_channel')
        async def forward_handler(callback: CallbackQuery):
            try:
                # Пересылаем сообщение в основной канал
                await self.bot.copy_message(
                    chat_id=Config.TARGET_CHANNEL_ID,
                    from_chat_id=callback.message.chat.id,
                    message_id=callback.message.message_id
                )
                await callback.answer("Новость переслана в канал")
            except Exception as e:
                logger.error(f"Forward error: {e}")
                await callback.answer("Ошибка при пересылке", show_alert=True)
                
    async def _edit_existing_message(self, text: str):
        """Редактирует существующее закрепленное сообщение"""
        await self.bot.edit_message_text(
            chat_id=Config.TARGET_CHANNEL_ID,
            message_id=self.pinned_message_id,
            text=text,
            parse_mode='HTML'
        )

    async def _process_pinned_update(self, callback: CallbackQuery, content_type: str):
        """Обновляет закреп с сохранением состояния"""
        try:
            now_str = datetime.now().strftime('%H:%M')
            raw_content = self._extract_content(callback.message)  # Исправленное имя метода
            
            # Обновляем только текущий раздел
            self.sections[content_type] = {
                'content': raw_content,
                'time': now_str,
                'emoji': self.sections[content_type]['emoji']
            }
            
            # Сохраняем в БД
            self.db.save_pinned_section(content_type, raw_content, now_str)
            
            # Формируем полное сообщение
            full_text = self._build_pinned_message()
            
            if self.pinned_message_id:
                try:
                    await self.bot.edit_message_text(
                        chat_id=Config.TARGET_CHANNEL_ID,
                        message_id=self.pinned_message_id,
                        text=full_text,
                        parse_mode='HTML'
                    )
                    logger.info(f"Updated pinned message {self.pinned_message_id}")
                except Exception as e:
                    logger.warning(f"Edit failed: {e}, creating new message")
                    await self._create_new_pinned_message(full_text)
            else:
                await self._create_new_pinned_message(full_text)
                
        except Exception as e:
            logger.error(f"Failed to update pinned: {e}")
            raise

    def _preserve_formatting(self, content: str, content_type: str) -> str:
        """Сохраняет эмодзи и форматирование в контенте"""
        if not content:
            return content
            
        # Для дивидендов сохраняем 💰 в каждой строке
        if content_type == 'dividends':
            lines = []
            for line in content.split('\n'):
                if line.strip() and not line.startswith('💰'):
                    lines.append(f"💰 {line}")
                else:
                    lines.append(line)
            return '\n'.join(lines)
        
        # Для других типов сохраняем цветовые маркеры
        return content.replace('🟢', '').replace('🔴', '').replace('⚪', '')

    def _build_pinned_message(self) -> str:
        """Собирает сообщение с сохранением всех эмодзи и форматирования"""
        sections = []
        titles = {
            'stocks': 'Рынок акций и индексы',
            'dividends': 'Ближайшие дивиденды',
            'commodities': 'Товарные активы',
            'crypto': 'Топ-10 криптовалют'
        }
        
        for content_type, data in self.sections.items():
            if data['content']:
                title = f"{data['emoji']} <b>{titles[content_type]} (обновлено {data['time']}):</b>"
                sections.append(f"{title}\n{data['content']}")
        
        return '\n\n'.join(sections)

    async def _create_new_pinned_message(self, text: str):
        """Создает новое закрепленное сообщение"""
        msg = await self.bot.send_message(
            chat_id=Config.TARGET_CHANNEL_ID,
            text=text,
            parse_mode='HTML'
        )
        await self.bot.pin_chat_message(
            chat_id=Config.TARGET_CHANNEL_ID,
            message_id=msg.message_id,
            disable_notification=True
        )
        self.pinned_message_id = msg.message_id
        self.db.save_pinned_message(msg.message_id)
        logger.info(f"Created new pinned message: {msg.message_id}")

    def _extract_raw_content(self, message: Message) -> str:
        """Извлечение чистого контента без заголовков"""
        text = message.caption if message.caption else message.text
        if not text:
            return ""
        
        # Удаляем первую строку (заголовок)
        if '\n' in text:
            #content = text.split('\n', 1)[1].strip()
            content = text.strip()  # убирает whitespace по краям, но сохраняет всё содержимое
        else:
            content = text
        
        # Удаляем возможные дублирующиеся эмодзи
        lines = []
        for line in content.split('\n'):
            clean_line = re.sub(r'^[^a-zA-Zа-яА-Я0-9]*', '', line.strip())
            if clean_line:
                lines.append(clean_line)
        
        return '\n'.join(lines)

    def _build_pinned_message(self) -> str:
        """Сборка полного сообщения с сохранением времени"""
        sections = []
        type_map = {
            'stocks': ('📊', 'Рынок акций и индексы'),
            'dividends': ('💵', 'Ближайшие дивиденды'),
            'commodities': ('🛢️', 'Товарные активы'),
            'crypto': ('💰', 'Топ-10 криптовалют')
        }
        
        for content_type, data in self.sections.items():
            if data['content']:
                emoji, title = type_map[content_type]
                time_str = data['time'] or "не обновлялось"
                sections.append(
                    f"{emoji} <b>{title} (обновлено {time_str}):</b>\n{data['content']}"
                )
        
        return '\n\n'.join(sections) if sections else "Нет данных для отображения"

    async def safe_send(self, text: str, image_path: str = None, content_type: str = None, parse_mode: str = 'HTML') -> bool:
        """Отправка сообщения с разной логикой для разных типов"""
        try:
            if parse_mode != 'HTML':
                text = self._clean_text(text)

            # Автодетект типа контента, если не указан
            if not content_type:
                content_type = self._detect_content_type(text)
    
            # Для всех типов (включая новости) отправляем с кнопками
            return await self._send_with_controls(text, image_path, content_type, parse_mode)

        except Exception as e:
            logger.error(f"Send error: {e}")
            return False
        
    async def _send_with_pin_controls(self, text: str, image_path: str, content_type: str, parse_mode: str) -> bool:
        """Отправка с кнопками управления закрепом"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📌 Обновить закреп", callback_data=f"update_pinned_{content_type}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data="delete_message")
        ]])
    
        try:
            if image_path and Path(image_path).exists():
                await self.bot.send_photo(
                    chat_id=Config.TELEGRAM_CHAT_ID,
                    photo=FSInputFile(image_path),
                    caption=text[:1020] if len(text) > 1020 else text,
                    parse_mode=parse_mode,
                    reply_markup=keyboard
                )
            else:
                await self.bot.send_message(
                    chat_id=Config.TELEGRAM_CHAT_ID,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=keyboard
                )
            return True
        finally:
            if image_path and Path(image_path).exists():
                Path(image_path).unlink(missing_ok=True)

    async def _forward_news(self, text: str, image_path: str, parse_mode: str) -> bool:
        """Простая пересылка новостей в канал"""
        try:
            if image_path and Path(image_path).exists():
                await self.bot.send_photo(
                    chat_id=Config.TARGET_CHANNEL_ID,
                    photo=FSInputFile(image_path),
                    caption=text[:1020] if len(text) > 1020 else text,
                    parse_mode=parse_mode
                )
            else:
                await self.bot.send_message(
                    chat_id=Config.TARGET_CHANNEL_ID,
                    text=text,
                    disable_web_page_preview=True,
                    parse_mode=parse_mode
                )
            return True
        finally:
            if image_path and Path(image_path).exists():
                Path(image_path).unlink(missing_ok=True)
                
    def _prepare_content(self, text: str, content_type: str) -> str:
        """Подготовка контента с сохранением эмодзи"""
        if not text:
            return ""
        
        # Только для дивидендов добавляем 💰 если отсутствует
        if content_type == 'dividends':
            lines = []
            for line in text.split('\n'):
                if line.strip() and not line.startswith(('💰', '🟢', '🔴', '⚪')):
                    lines.append(f"💰 {line}")
                else:
                    lines.append(line)
            return '\n'.join(lines)
        
        return text
                
    async def _send_with_controls(self, text: str, image_path: str, content_type: str, parse_mode: str) -> bool:
        """Отправка сообщения с соответствующими кнопками"""
        text = self._prepare_content(text, content_type)
        # Для новостей - кнопка "Переслать в канал"
        if content_type == 'news':
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📌 Переслать в канал", callback_data="forward_to_channel"),
                InlineKeyboardButton(text="🗑 Удалить", callback_data="delete_message")
            ]])
        # Для финансовых данных - кнопка "Обновить закреп"
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📌 Обновить закреп", callback_data=f"update_pinned_{content_type}"),
                InlineKeyboardButton(text="🗑 Удалить", callback_data="delete_message")
            ]])

        try:
            if image_path and Path(image_path).exists():
                await self.bot.send_photo(
                    chat_id=Config.TELEGRAM_CHAT_ID,
                    photo=FSInputFile(image_path),
                    caption=text[:1020] if len(text) > 1020 else text,
                    parse_mode=parse_mode,
                    reply_markup=keyboard
                )
            else:
                await self.bot.send_message(
                    chat_id=Config.TELEGRAM_CHAT_ID,
                    text=text,
                    parse_mode=parse_mode,
                    disable_web_page_preview=True,
                    reply_markup=keyboard
                )
            return True
        finally:
            if image_path and Path(image_path).exists():
                Path(image_path).unlink(missing_ok=True)

    def _detect_content_type(self, text: str) -> str:
        """Определение типа контента с учетом новостей"""
        text_lower = text.lower()
        if "новост" in text_lower or "рбк" in text_lower or "тасс" in text_lower:
            return 'news'
        elif "рынок акций" in text_lower or "индекс" in text_lower:
            return 'stocks'
        elif "дивиденд" in text_lower:
            return 'dividends'
        elif "товарн" in text_lower or "нефть" in text_lower:
            return 'commodities'
        elif "крипт" in text_lower:
            return 'crypto'
        return 'other'