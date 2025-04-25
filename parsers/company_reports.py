import re
import logging
import hashlib
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from database import NewsDatabase
from services.telegram_client import TelegramClient
from utils.html_formatter import HTMLFormatter
from aiogram import Bot, Dispatcher, F
import asyncio

logger = logging.getLogger(__name__)

class CompanyReportsParser:
    def __init__(self, dp: Dispatcher, db: NewsDatabase):
        self.tg = TelegramClient(dp, db)
        self.db = db
        self.timeout = 30  # seconds
        
        # Словарь компаний: {домен: (название, тикер)}
        self.companies = {
            'inarctica.com': ('Инарктика', 'AQUA'),
            'mmk.ru': ('ММК', 'MAGN'),  # Добавляем ММК с тикером MAGN
            'vk.company': ('VK', 'VKCO'),  # Добавляем VK с тикером VKCO
            'sollers-auto.com': ('СОЛЛЕРС', 'SVAV'),  # Добавляем СОЛЛЕРС с тикером SVAV
            'ptsecurity.com': ('Positive Technologies', 'POSI'),  # Добавляем Positive Technologies
            'seligdar.ru': ('Селигдар', 'SELG'),  # Добавляем Селигдар
            'ozonpharm.ru': ('Озон Фармацевтика', 'OZON'),
            # Добавьте другие компании по аналогии
            # 'company2.com': ('Название2', 'TICKER2'),
        }
    
    async def parse_ozonpharm(self):
        """Парсинг отчетов Озон Фармацевтика с фокусом на ключевые показатели"""
        from playwright.async_api import async_playwright
        
        base_url = "https://ozonpharm.ru/news/press-releases/"
        news = []
        MAX_MESSAGE_LENGTH = 4000
        
        try:
            logger.info(f"Начинаем парсинг Озон Фармацевтика: {base_url}")
            
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    viewport={'width': 1920, 'height': 1080}
                )
                page = await context.new_page()
                
                await page.goto(base_url, timeout=60000)
                await page.wait_for_selector('a.news-results__item.news-item', timeout=30000)
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                news_blocks = soup.select('a.news-results__item.news-item')
                logger.info(f"Найдено {len(news_blocks)} новостных блоков Озон Фармацевтика")
                
                for block in news_blocks[:5]:  # Ограничиваем количество
                    try:
                        # Извлекаем основные метаданные
                        date_block = block.select_one('div.z-date__card')
                        title_block = block.select_one('p.news-item__title span')
                        link_block = block
                        
                        if not all([date_block, title_block, link_block]):
                            logger.warning("Неполные данные в новостном блоке")
                            continue
                        
                        # Форматируем дату
                        day = date_block.select_one('span.z-date__day').get_text(strip=True)
                        month_elements = date_block.select('span.z-date__other')
                        month = month_elements[0].get_text(strip=True) if month_elements else ''
                        year = month_elements[1].get_text(strip=True) if len(month_elements) > 1 else ''
                        date_str = f"{day} {month} {year}".strip()
                        
                        title = title_block.get_text(strip=True)
                        news_url = urljoin(base_url, link_block['href'])
                        news_id = hashlib.md5(news_url.encode()).hexdigest()
                        
                        logger.info(f"Обработка: {date_str} | {title[:50]}...")
                        
                        # Проверка дубликатов и фильтрация по ключевым словам
                        if (await self.db.is_news_exists(news_id) or 
not any(kw in title.lower() for kw in ['отчет', 'результат', 'финанс', 'дивиденд'])):
                            continue
                        
                        # Переходим на страницу новости
                        await page.goto(news_url, timeout=30000)
                        try:
                            await page.wait_for_selector('article.detail-page', timeout=20000)
                        except Exception as e:
                            logger.warning(f"Не удалось загрузить страницу: {news_url}")
                            continue
                        
                        # Получаем и анализируем контент
                        news_content = await page.content()
                        news_soup = BeautifulSoup(news_content, 'html.parser')
                        content_block = news_soup.select_one('article.detail-page')
                        
                        if not content_block:
                            continue
                        
                        # 1. Извлекаем ключевые показатели из таблицы
                        financial_data = []
                        table = content_block.select_one('div.z-table__container table')
                        if table:
                            headers = [th.get_text(strip=True) for th in table.select('thead th')]
                            for row in table.select('tbody tr'):
                                cells = row.select('td')
                                if len(cells) == len(headers):
                                    metric = cells[0].get_text(strip=True)
                                    values = [cell.get_text(strip=True) for cell in cells[1:]]
                                    financial_data.append(f"{metric}: {', '.join(values)}")
                        
                        # 2. Извлекаем основные результаты из списков
                        key_results = []
                        for li in content_block.select('li.z-list-item'):
                            text = li.get_text(' ', strip=True)
                            if any(kw in text.lower() for kw in ['выручк', 'ebitda', 'прибыл', 'рентабельност']):
                                key_results.append(f"• {text}")
                        
                        # Формируем компактное сообщение
                        message_lines = [
                            f"<b>#OZON #отчетность</b>",
                            f"<b>{title}</b> ({date_str})",
                            "",
                            "<b>Ключевые показатели:</b>",
                            *financial_data[:8],  # Ограничиваем количество показателей
                            "",
                            "<b>Основные результаты:</b>",
                            *key_results[:5]      # Ограничиваем количество результатов
                        ]
                        
                        # Удаляем пустые строки и обрезаем длинные значения
                        message_lines = [line for line in message_lines if line.strip()]
                        message = '\n'.join(message_lines[:25])  # Жесткое ограничение строк
                        
                        if len(message) > MAX_MESSAGE_LENGTH:
                            message = message[:MAX_MESSAGE_LENGTH-50] + "..."
                        
                        message += f"\n\n<a href='{news_url}'>Подробнее</a>"
                        
                        news.append(message)
                        await self.db.add_news(news_id, 'company_reports', title, news_url)
                        logger.info(f"Добавлен отчет: {title[:30]}...")
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки: {str(e)}", exc_info=True)
                        continue
                        
                await browser.close()
                
        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
        
        logger.info(f"Завершено. Найдено {len(news)} новых отчетов")
        return news
        
    async def parse_seligdar(self):
        """Парсинг отчетов Селигдар с использованием Playwright"""
        from playwright.async_api import async_playwright
        
        base_url = "https://seligdar.ru/media/news/"
        news = []
        MAX_MESSAGE_LENGTH = 4000  # Максимальная длина сообщения
        
        try:
            logger.info(f"Начинаем парсинг Селигдар: {base_url}")
            
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    viewport={'width': 1920, 'height': 1080}
                )
                page = await context.new_page()
                
                await page.goto(base_url, timeout=60000)
                await page.wait_for_selector('ul.list-dates', timeout=15000)
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                news_blocks = soup.select('ul.list-dates > li > a')
                logger.info(f"Найдено {len(news_blocks)} новостных блоков Селигдар")
                
                for block in news_blocks[:10]:  # Ограничиваем количество
                    try:
                        date_block = block.select_one('span.date')
                        title = block.get_text(strip=True).replace(date_block.get_text(strip=True), '').strip()
                        
                        if not all([date_block, title]):
                            logger.warning("Неполные данные в новостном блоке Селигдар")
                            continue
                        
                        date_str = date_block.get_text(strip=True)
                        news_url = urljoin(base_url, block['href'])
                        news_id = hashlib.md5(news_url.encode()).hexdigest()
                        
                        logger.info(f"Обработка новости Селигдар: {date_str} | {title[:50]}...")
                        
                        if await self.db.is_news_exists(news_id):
                            logger.debug("Новость Селигдар уже в базе")
                            continue
                        
                        finance_keywords = [
                            'отчет', 'результат', 'прибыль', 'выручк', 'EBITDA', 
                            'дивиденд', 'финансов', 'квартал', 'год', 'МСФО',
                            'консолидированн', 'рентабельность', 'SELG',
                            'операционные', 'производство', 'продаж', 'добыч'
                        ]
                        if not any(kw.lower() in title.lower() for kw in finance_keywords):
                            logger.debug(f"Не финансовый отчет Селигдар: {title}")
                            continue
                        
                        await page.goto(news_url, timeout=30000)
                        try:
                            await page.wait_for_selector('div.block_text', timeout=10000)
                        except Exception as e:
                            logger.warning(f"Не удалось загрузить страницу новости: {news_url}")
                            continue
                        
                        news_content = await page.content()
                        news_soup = BeautifulSoup(news_content, 'html.parser')
                        
                        content_block = news_soup.select_one('div.block_text')
                        if not content_block:
                            logger.warning(f"Не найден контент новости: {news_url}")
                            continue
                        
                        # Собираем все элементы контента с приоритетами
                        content_elements = []
                        paragraphs = content_block.find_all(['p', 'h2', 'h3', 'ul', 'table'])
                        
                        for p in paragraphs:
                            if p.name in ['p', 'h2', 'h3']:
                                text = p.get_text(strip=True)
                                if not text or len(text) < 20:
                                    continue
                                    
                                priority = 4  # Обычный текст
                                if any(word in text.lower() for word in ['руб', '$', 'млрд', 'млн', '%', 'EBITDA']):
                                    priority = 1  # Финансовые данные
                                elif p.name in ['h2', 'h3']:
                                    priority = 2  # Заголовки
                                elif 'дивизион' in text.lower():
                                    priority = 3  # Названия дивизионов
                                    
                                content_elements.append({
                                    'text': text,
                                    'tag': p.name,
                                    'priority': priority
                                })
                            
                            elif p.name == 'ul':
                                for li in p.find_all('li'):
                                    item_text = li.get_text(strip=True)
                                    if not item_text:
                                        continue
                                        
                                    priority = 3  # Пункты списка
                                    if any(word in item_text.lower() for word in ['руб', '$', 'млрд', 'млн', '%']):
                                        priority = 1
                                        
                                    content_elements.append({
                                        'text': item_text,
                                        'tag': 'li',
                                        'priority': priority
                                    })
                            
                            elif p.name == 'table':
                                # Обрабатываем таблицы с финансовыми показателями
                                rows = p.find_all('tr')
                                if len(rows) > 1:
                                    headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
                                    for row in rows[1:]:
                                        cells = row.find_all('td')
                                        if len(cells) == len(headers):
                                            row_text = ' | '.join([f"{headers[i]}: {cell.get_text(strip=True)}" 
                                                                for i, cell in enumerate(cells)])
                                            content_elements.append({
                                                'text': row_text,
                                                'tag': 'table_row',
                                                'priority': 1  # Высокий приоритет для табличных данных
                                            })
                        
                        if not content_elements:
                            logger.warning(f"Не удалось извлечь содержание отчета Селигдар: {news_url}")
                            continue
                        
                        # Формируем сообщение с учетом приоритетов
                        message_parts = [
                            f"<b>#SELG #отчетность</b>",
                            f"<b>{title}</b> ({date_str})"
                        ]
                        current_length = sum(len(part) for part in message_parts)
                        
                        # Сортируем элементы по приоритету (сначала важные)
                        content_elements.sort(key=lambda x: x['priority'])
                        
                        for element in content_elements:
                            new_part = ""
                            if element['tag'] in ['h2', 'h3']:
                                new_part = f"<b>{element['text']}</b>"
                            elif element['tag'] == 'table_row':
                                new_part = f"• {element['text']}"
                            else:
                                # Для обычного текста и списков ограничиваем длину
                                text = element['text']
                                if len(text) > 200:
                                    sentences = re.split(r'(?<=[.!?])\s+', text)
                                    if len(sentences) > 1:
                                        text = ' '.join(sentences[:2]) + '...'
                                    else:
                                        text = text[:200] + '...'
                                new_part = f"• {text}" if element['tag'] == 'li' else text
                            
                            # Проверяем, не превысим ли лимит
                            if current_length + len(new_part) + 10 < MAX_MESSAGE_LENGTH:
                                message_parts.append(new_part)
                                current_length += len(new_part)
                            else:
                                # Добавляем только если это критически важная информация
                                if element['priority'] <= 2 and current_length + 100 < MAX_MESSAGE_LENGTH:
                                    short_part = f"• {element['text'][:150]}..." if len(element['text']) > 150 else f"• {element['text']}"
                                    message_parts.append(short_part)
                                    current_length += len(short_part)
                                break
                        
                        message_parts.append(f"<a href='{news_url}'>— Селигдар</a>")
                        
                        news_item = '\n'.join(message_parts)
                        news.append(news_item)
                        await self.db.add_news(news_id, 'company_reports', title, news_url)
                        logger.info(f"Новость Селигдар успешно добавлена: {title}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки новости Селигдар: {str(e)}", exc_info=True)
                        continue
                        
                await browser.close()
                
        except Exception as e:
            logger.error(f"Критическая ошибка парсинга Селигдар: {str(e)}", exc_info=True)
        
        logger.info(f"Парсинг Селигдар завершен. Найдено {len(news)} новых отчетов")
        return news
        
    async def parse_pt(self):
        """Парсинг отчетов Positive Technologies с использованием Playwright"""
        from playwright.async_api import async_playwright
        
        base_url = "https://group.ptsecurity.com/ru/news/"
        news = []
        MAX_MESSAGE_LENGTH = 4000  # Максимальная длина сообщения
        
        try:
            logger.info(f"Начинаем парсинг Positive Technologies: {base_url}")
            
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    viewport={'width': 1920, 'height': 1080}
                )
                page = await context.new_page()
                
                await page.goto(base_url, timeout=60000)
                await page.wait_for_selector('div.grid-cols-5', timeout=15000)
                await page.wait_for_timeout(2000)
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                news_blocks = soup.select('div.grid-cols-5 > div.col-span-3 > a.listing-item')
                logger.info(f"Найдено {len(news_blocks)} новостных блоков Positive Technologies")
                
                for block in news_blocks[:10]:
                    try:
                        date_block = block.select_one('div.listing-item__date')
                        title_block = block.select_one('h2.listing-item__title')
                        
                        if not all([date_block, title_block]):
                            logger.warning("Неполные данные в новостном блоке Positive Technologies")
                            continue
                        
                        date_str = date_block.get_text(strip=True)
                        title = title_block.get_text(strip=True)
                        news_url = urljoin(base_url, block['href'])
                        news_id = hashlib.md5(news_url.encode()).hexdigest()
                        
                        logger.info(f"Обработка новости Positive Technologies: {date_str} | {title[:50]}...")
                        
                        if await self.db.is_news_exists(news_id):
                            logger.debug("Новость Positive Technologies уже в базе")
                            continue
                        
                        finance_keywords = [
                            'отчет', 'результат', 'прибыль', 'выручк', 'EBITDA', 
                            'дивиденд', 'финансов', 'квартал', 'год', 'МСФО',
                            'консолидированн', 'рентабельность', 'POSI'
                        ]
                        if not any(kw.lower() in title.lower() for kw in finance_keywords):
                            logger.debug(f"Не финансовый отчет Positive Technologies: {title}")
                            continue
                        
                        await page.goto(news_url, timeout=30000)
                        try:
                            await page.wait_for_selector('article', timeout=10000)
                        except Exception as e:
                            logger.warning(f"Не удалось загрузить страницу новости: {news_url}")
                            continue
                        
                        news_content = await page.content()
                        news_soup = BeautifulSoup(news_content, 'html.parser')
                        
                        content_block = news_soup.select_one('article')
                        if not content_block:
                            logger.warning(f"Не найден контент новости: {news_url}")
                            continue
                        
                        # Собираем все элементы контента с приоритетами
                        content_elements = []
                        paragraphs = content_block.find_all(['p', 'h2', 'h3', 'blockquote', 'div.links-block'])
                        
                        for p in paragraphs:
                            if 'Контакты для' in p.get_text():
                                continue
                                
                            text = p.get_text(strip=True)
                            if not text or len(text) < 30 or text.startswith(('<', '[')):
                                continue
                                
                            # Приоритеты: 1 - цифры/финансы, 2 - заголовки, 3 - цитаты, 4 - обычный текст
                            priority = 4
                            if any(word in text.lower() for word in ['руб', '$', 'млрд', 'млн', '%', 'EBITDA']):
                                priority = 1
                            elif p.name in ['h2', 'h3']:
                                priority = 2
                            elif p.name == 'blockquote':
                                priority = 3
                                
                            content_elements.append({
                                'text': text,
                                'tag': p.name,
                                'priority': priority
                            })
                        
                        # Добавляем ссылки на документы отдельно
                        links_block = content_block.select_one('div.links-block')
                        if links_block:
                            for link in links_block.find_all('a'):
                                content_elements.append({
                                    'text': link.get_text(strip=True),
                                    'tag': 'a',
                                    'priority': 1,  # Высокий приоритет для ссылок на документы
                                    'href': link['href']
                                })
                        
                        # Формируем сообщение с учетом приоритетов
                        message_parts = [
                            f"<b>#POSI #отчетность</b>",
                            f"<b>{title}</b> ({date_str})"
                        ]
                        current_length = sum(len(part) for part in message_parts)
                        
                        # Сортируем элементы по приоритету (сначала важные)
                        content_elements.sort(key=lambda x: x['priority'])
                        
                        for element in content_elements:
                            new_part = ""
                            if element['tag'] in ['h2', 'h3']:
                                new_part = f"<b>{element['text']}</b>"
                            elif element['tag'] == 'blockquote':
                                new_part = f"📌 {element['text']}"
                            elif element['tag'] == 'a':
                                new_part = f"📄 <a href='{element['href']}'>{element['text']}</a>"
                            else:
                                # Для обычного текста берем только первые 2 предложения
                                sentences = re.split(r'(?<=[.!?])\s+', element['text'])
                                if len(sentences) > 2:
                                    new_part = f"• {' '.join(sentences[:2])}..."
                                else:
                                    new_part = f"• {element['text']}"
                            
                            # Проверяем, не превысим ли лимит
                            if current_length + len(new_part) + 10 < MAX_MESSAGE_LENGTH:  # +10 для запаса
                                message_parts.append(new_part)
                                current_length += len(new_part)
                            else:
                                # Добавляем только если это критически важная информация
                                if element['priority'] == 1 and current_length + 50 < MAX_MESSAGE_LENGTH:
                                    short_part = f"• {element['text'][:100]}..." if len(element['text']) > 100 else f"• {element['text']}"
                                    message_parts.append(short_part)
                                    current_length += len(short_part)
                                break
                        
                        message_parts.append(f"<a href='{news_url}'>— Positive Technologies</a>")
                        
                        news_item = '\n'.join(message_parts)
                        news.append(news_item)
                        await self.db.add_news(news_id, 'company_reports', title, news_url)
                        logger.info(f"Новость Positive Technologies успешно добавлена: {title}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки новости Positive Technologies: {str(e)}", exc_info=True)
                        continue
                        
                await browser.close()
                
        except Exception as e:
            logger.error(f"Критическая ошибка парсинга Positive Technologies: {str(e)}", exc_info=True)
        
        logger.info(f"Парсинг Positive Technologies завершен. Найдено {len(news)} новых отчетов")
        return news
        
    async def parse_sollers(self):
        """Парсинг отчетов СОЛЛЕРС с использованием Playwright"""
        from playwright.async_api import async_playwright
        
        base_url = "https://sollers-auto.com/press-center/news/"
        news = []
        
        try:
            logger.info(f"Начинаем парсинг СОЛЛЕРС: {base_url}")
            
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = await context.new_page()
                
                await page.goto(base_url, timeout=60000)
                await page.wait_for_selector('div.news__item', timeout=15000)
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                news_blocks = soup.select('div.news__item')
                logger.info(f"Найдено {len(news_blocks)} новостных блоков СОЛЛЕРС")
                
                for block in news_blocks[:10]:
                    try:
                        date_block = block.select_one('p.news-item__date')
                        title_block = block.select_one('a.news-item__title')
                        preview_block = block.select_one('p.news-item__prevText')
                        
                        if not all([date_block, title_block, preview_block]):
                            logger.warning("Неполные данные в новостном блоке СОЛЛЕРС")
                            continue
                        
                        date_str = date_block.get_text(strip=True)
                        title = title_block.get_text(strip=True)
                        news_url = urljoin(base_url, title_block['href'])
                        news_id = hashlib.md5(news_url.encode()).hexdigest()
                        
                        logger.info(f"Обработка новости СОЛЛЕРС: {date_str} | {title[:50]}...")
                        
                        if await self.db.is_news_exists(news_id):
                            logger.debug("Новость СОЛЛЕРС уже в базе")
                            continue
                        
                        finance_keywords = [
                            'отчет', 'результат', 'прибыль', 'выручк', 'EBITDA', 
                            'дивиденд', 'финансов', 'год', 'МСФО', 'консолидированн',
                            'рентабельность', 'SVAV'
                        ]
                        if not any(kw.lower() in title.lower() for kw in finance_keywords):
                            logger.debug(f"Не финансовый отчет СОЛЛЕРС: {title}")
                            continue
                        
                        await page.goto(news_url, timeout=30000)
                        await page.wait_for_selector('div.news-content__wrapper', timeout=10000)
                        
                        news_content = await page.content()
                        news_soup = BeautifulSoup(news_content, 'html.parser')
                        
                        content_block = news_soup.select_one('div.news-content__wrapper')
                        report_items = []
                        
                        if content_block:
                            paragraphs = content_block.find_all(['p', 'b'])
                            for p in paragraphs:
                                text = p.get_text(strip=True)
                                if len(text) > 30 and not text.startswith(('<', '[')):
                                    report_items.append(f"• {text}")
                            
                            table = content_block.find('table')
                            if table:
                                unit = table.find('thead').find('th').get_text(strip=True)
                                rows = table.find_all('tr')[1:]
                                for row in rows:
                                    cells = row.find_all('td')
                                    if len(cells) == 2:
                                        key = cells[0].get_text(strip=True)
                                        value = cells[1].get_text(strip=True)
                                        formatted_value = f"{value} {unit}" if '%' not in value else value
                                        report_items.append(f"  - {key}: {formatted_value}")
                        
                        if not report_items:
                            logger.warning(f"Не удалось извлечь содержание отчета СОЛЛЕРС: {news_url}")
                            continue
                        
                        message_lines = [
                            f"<b>#SVAV #отчетность</b>",
                            f"<b>{title}</b> ({date_str})",
                            *report_items[:8],
                            f"<a href='{news_url}'>— СОЛЛЕРС</a>"
                        ]
                        
                        news_item = '\n'.join(message_lines)
                        news.append(news_item)
                        await self.db.add_news(news_id, 'company_reports', title, news_url)
                        logger.info(f"Новость СОЛЛЕРС успешно добавлена: {title}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки новости СОЛЛЕРС: {str(e)}", exc_info=True)
                        continue
                        
                await browser.close()
                
        except Exception as e:
            logger.error(f"Критическая ошибка парсинга СОЛЛЕРС: {str(e)}", exc_info=True)
        
        logger.info(f"Парсинг СОЛЛЕРС завершен. Найдено {len(news)} новых отчетов")
        return news
        
    async def parse_vk(self):
        """Парсинг отчетов VK с использованием Playwright"""
        from playwright.async_api import async_playwright
        
        base_url = "https://vk.company/ru/press/releases/"
        news = []
        
        try:
            logger.info(f"Начинаем парсинг VK: {base_url}")
            
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = await context.new_page()
                
                await page.goto(base_url, timeout=60000)
                await page.wait_for_selector('div.Publications_publicationItem__ICFNd', timeout=15000)
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                news_blocks = soup.select('div.Publications_publicationItem__ICFNd')
                logger.info(f"Найдено {len(news_blocks)} новостных блоков VK")
                
                for block in news_blocks[:10]:  # Ограничиваем количество
                    try:
                        # Извлекаем основные данные
                        date_block = block.select_one('div.Publications_publicationSubtitle__e297T')
                        title_block = block.select_one('div.Publications_publicationTitle__oKOtT')
                        link_block = block.select_one('a.Publications_publication__Ehhcu')
                        
                        if not all([date_block, title_block, link_block]):
                            logger.warning("Неполные данные в новостном блоке VK")
                            continue
                        
                        date_str = date_block.get_text(strip=True)
                        title = title_block.get_text(strip=True)
                        news_url = urljoin(base_url, link_block['href'])
                        news_id = hashlib.md5(news_url.encode()).hexdigest()
                        
                        logger.info(f"Обработка новости VK: {date_str} | {title[:50]}...")
                        
                        # Проверка дубликатов
                        if await self.db.is_news_exists(news_id):
                            logger.debug("Новость VK уже в базе")
                            continue
                        
                        # Фильтрация по финансовым ключевым словам
                        finance_keywords = [
                            'отчет', 'результат', 'прибыль', 'выручк', 'EBITDA', 
                            'дивиденд', 'финансов', 'квартал', 'год', 'месяц',
                            'операционные', 'релиз', 'анализ'
                        ]
                        if not any(kw.lower() in title.lower() for kw in finance_keywords):
                            logger.debug(f"Не финансовый отчет VK: {title}")
                            continue
                        
                        # Переходим на страницу новости
                        await page.goto(news_url, timeout=30000)
                        await page.wait_for_selector('div.publication-content', timeout=10000)
                        
                        # Получаем контент новости
                        news_content = await page.content()
                        news_soup = BeautifulSoup(news_content, 'html.parser')
                        
                        # Извлекаем основные пункты отчета
                        content_block = news_soup.select_one('div.publication-content')
                        report_items = []
                        
                        if content_block:
                            # Собираем все важные пункты (заголовки и списки)
                            strong_headers = content_block.find_all(['strong', 'p'])
                            ul_blocks = content_block.find_all('ul')
                            
                            # Добавляем заголовки
                            for header in strong_headers:
                                text = header.get_text(strip=True)
                                if len(text) > 30 and not text.startswith(('[')):  # Фильтруем короткие и сноски
                                    report_items.append(f"• {text}")
                            
                            # Добавляем пункты списков
                            for ul in ul_blocks:
                                for li in ul.find_all('li'):
                                    item_text = li.get_text(strip=True)
                                    if item_text:  # Игнорируем пустые пункты
                                        report_items.append(f"  - {item_text}")
                        
                        if not report_items:
                            logger.warning(f"Не удалось извлечь содержание отчета VK: {news_url}")
                            continue
                        
                        # Формируем сообщение
                        message_lines = [
                            f"<b>#VKCO #отчетность</b>",
                            f"<b>{title}</b> ({date_str})",
                            *report_items[:8],  # Ограничиваем количество пунктов
                            f"<a href='{news_url}'>— VK</a>"
                        ]
                        
                        news_item = '\n'.join(message_lines)
                        news.append(news_item)
                        await self.db.add_news(news_id, 'company_reports', title, news_url)
                        logger.info(f"Новость VK успешно добавлена: {title}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки новости VK: {str(e)}", exc_info=True)
                        continue
                        
                await browser.close()
                
        except Exception as e:
            logger.error(f"Критическая ошибка парсинга VK: {str(e)}", exc_info=True)
        
        logger.info(f"Парсинг VK завершен. Найдено {len(news)} новых отчетов")
        return news
        
    async def parse_mmk(self):
        """Парсинг отчетов ММК с использованием Playwright"""
        from playwright.async_api import async_playwright
        
        base_url = "https://mmk.ru/ru/press-center/news/"
        news = []
        
        try:
            logger.info(f"Начинаем парсинг ММК: {base_url}")
            
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = await context.new_page()
                
                await page.goto(base_url, timeout=60000)
                await page.wait_for_selector('div.card-news-list__card', timeout=15000)
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                news_blocks = soup.select('div.card-news-list__card')
                logger.info(f"Найдено {len(news_blocks)} новостных блоков ММК")
                
                for block in news_blocks[:10]:  # Ограничиваем количество
                    try:
                        # Извлекаем основные данные
                        date_block = block.select_one('span.card-article__date')
                        title_block = block.select_one('div.card-article__title')
                        link_block = block.select_one('a.card-article__link')
                        
                        if not all([date_block, title_block, link_block]):
                            logger.warning("Неполные данные в новостном блоке ММК")
                            continue
                        
                        date_str = date_block.get_text(strip=True)
                        title = title_block.get_text(strip=True)
                        news_url = urljoin(base_url, link_block['href'])
                        news_id = hashlib.md5(news_url.encode()).hexdigest()
                        
                        logger.info(f"Обработка новости ММК: {date_str} | {title[:50]}...")
                        
                        # Проверка дубликатов
                        if await self.db.is_news_exists(news_id):
                            logger.debug("Новость ММК уже в базе")
                            continue
                        
                        # Фильтрация по финансовым ключевым словам
                        finance_keywords = [
                            'отчет', 'результат', 'прибыль', 'выручк', 'EBITDA', 
                            'дивиденд', 'финансов', 'квартал', 'год', 'месяц'
                        ]
                        if not any(kw.lower() in title.lower() for kw in finance_keywords):
                            logger.debug(f"Не финансовый отчет ММК: {title}")
                            continue
                        
                        # Переходим на страницу новости
                        await page.goto(news_url, timeout=30000)
                        await page.wait_for_selector('div.text-editor__content', timeout=10000)
                        
                        # Получаем контент новости
                        news_content = await page.content()
                        news_soup = BeautifulSoup(news_content, 'html.parser')
                        
                        # Извлекаем основные пункты отчета
                        content_block = news_soup.select_one('div.text-editor__content')
                        report_items = []
                        
                        if content_block:
                            # Собираем все пункты списка (если есть)
                            ul_blocks = content_block.find_all('ul')
                            for ul in ul_blocks:
                                for li in ul.find_all('li'):
                                    item_text = li.get_text(strip=True)
                                    report_items.append(f"• {item_text}")
                            
                            # Если нет списка, берем первые 3 абзаца после заголовка
                            if not report_items:
                                paragraphs = content_block.find_all('p')
                                for p in paragraphs[:3]:
                                    text = p.get_text(strip=True)
                                    if text and len(text) > 20:  # Игнорируем короткие абзацы
                                        report_items.append(text)
                        
                        if not report_items:
                            logger.warning(f"Не удалось извлечь содержание отчета ММК: {news_url}")
                            continue
                        
                        # Формируем сообщение
                        message_lines = [
                            f"<b>#MAGN #отчетность</b>",
                            f"<b>{title}</b> ({date_str})",
                            *report_items[:5],  # Ограничиваем количество пунктов
                            f"<a href='{news_url}'>— ММК</a>"
                        ]
                        
                        news_item = '\n'.join(message_lines)
                        news.append(news_item)
                        await self.db.add_news(news_id, 'company_reports', title, news_url)
                        logger.info(f"Новость ММК успешно добавлена: {title}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки новости ММК: {str(e)}", exc_info=True)
                        continue
                        
                await browser.close()
                
        except Exception as e:
            logger.error(f"Критическая ошибка парсинга ММК: {str(e)}", exc_info=True)
        
        logger.info(f"Парсинг ММК завершен. Найдено {len(news)} новых отчетов")
        return news

    async def parse_inarctica(self):
        """Парсинг отчетов Инарктики с использованием Playwright"""
        from playwright.async_api import async_playwright
        
        base_url = "https://inarctica.com/media/news/"
        news = []
        
        try:
            logger.info(f"Начинаем парсинг Инарктики: {base_url}")
            
            async with async_playwright() as pw:
                # Запускаем браузер в headless режиме
                browser = await pw.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = await context.new_page()
                
                # Переходим на страницу и ждем загрузки контента
                await page.goto(base_url, timeout=60000)
                await page.wait_for_selector('article.news-block', timeout=15000)
                
                # Получаем HTML после загрузки динамического контента
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                # Находим все новостные блоки
                news_blocks = soup.select('article.news-block')
                logger.info(f"Найдено {len(news_blocks)} новостных блоков")
                
                for block in news_blocks[:10]:  # Ограничиваем количество
                    try:
                        # Извлекаем данные
                        date_block = block.select_one('div.news-block__date')
                        title_block = block.select_one('h3.h3')
                        link_block = block.select_one('a.btn-accent-link')
                        
                        if not all([date_block, title_block, link_block]):
                            logger.warning("Неполные данные в новостном блоке")
                            continue
                        
                        date_str = ' '.join(date_block.stripped_strings)
                        title = title_block.get_text(strip=True)
                        news_url = urljoin(base_url, link_block['href'])
                        news_id = hashlib.md5(news_url.encode()).hexdigest()
                        
                        logger.info(f"Обработка новости: {date_str} | {title[:50]}...")
                        
                        # Проверка дубликатов
                        if await self.db.is_news_exists(news_id):
                            logger.debug("Новость уже в базе")
                            continue
                        
                        # Фильтрация по типу новости
                        finance_keywords = ['отчет', 'результат', 'прибыль', 'выручк', 'EBITDA', 'дивиденд']
                        if not any(kw.lower() in title.lower() for kw in finance_keywords):
                            logger.debug("Не финансовый отчет, пропускаем")
                            continue
                        
                        # Переходим на страницу новости
                        await page.goto(news_url, timeout=30000)
                        await page.wait_for_selector('div.article__content', timeout=10000)
                        
                        # Получаем контент новости
                        news_content = await page.content()
                        news_soup = BeautifulSoup(news_content, 'html.parser')
                        
                        # Извлекаем основные пункты отчета
                        content_block = news_soup.select_one('div.article__content')
                        report_items = []
                        
                        if content_block:
                            ul_block = content_block.find('ul')
                            if ul_block:
                                for li in ul_block.find_all('li'):
                                    item_text = li.get_text(strip=True)
                                    report_items.append(f"• {item_text}")
                            else:
                                first_p = content_block.find('p')
                                if first_p:
                                    report_items.append(first_p.get_text(strip=True))
                        
                        if not report_items:
                            logger.warning("Не удалось извлечь содержание отчета")
                            continue
                        
                        # Формируем сообщение
                        message_lines = [
                            f"<b>#AQUA #отчетность</b>",
                            f"<b>{title}</b> ({date_str})",
                            *report_items[:5],  # Ограничиваем количество пунктов
                            f"<a href='{news_url}'>— Инарктика</a>"
                        ]
                        
                        news_item = '\n'.join(message_lines)
                        news.append(news_item)
                        await self.db.add_news(news_id, 'company_reports', title, news_url)
                        logger.info("Новость успешно добавлена")
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки новости: {str(e)}", exc_info=True)
                        continue
                        
                await browser.close()
                
        except Exception as e:
            logger.error(f"Критическая ошибка парсинга: {str(e)}", exc_info=True)
        
        logger.info(f"Парсинг завершен. Найдено {len(news)} новых отчетов")
        return news

    

    async def parse(self):
        """Основной метод парсинга всех компаний"""
        has_news = False
        
        # Парсинг Инарктики
        inarctica_news = await self.parse_inarctica()
        if isinstance(inarctica_news, list) and inarctica_news:
            for news_item in inarctica_news:
                await self.tg.safe_send(f"📊 <b>ОТЧЕТЫ КОМПАНИЙ</b>\n{news_item}", 
                                    parse_mode='HTML',
                                    content_type='news')
                await asyncio.sleep(4)
                has_news = True
        
        # Парсинг ММК
        mmk_news = await self.parse_mmk()
        if isinstance(mmk_news, list) and mmk_news:
            for news_item in mmk_news:
                await self.tg.safe_send(f"📊 <b>ОТЧЕТЫ КОМПАНИЙ</b>\n{news_item}", 
                                    parse_mode='HTML',
                                    content_type='news')
                await asyncio.sleep(4)
                has_news = True
                
        # Парсинг VK
        vk_news = await self.parse_vk()
        if isinstance(vk_news, list) and vk_news:
            for news_item in vk_news:
                await self.tg.safe_send(f"📊 <b>ОТЧЕТЫ КОМПАНИЙ</b>\n{news_item}", 
                                    parse_mode='HTML',
                                    content_type='news')
                await asyncio.sleep(4)
                has_news = True
                
        # Парсинг СОЛЛЕРС
        sollers_news = await self.parse_sollers()
        if isinstance(sollers_news, list) and sollers_news:
            for news_item in sollers_news:
                await self.tg.safe_send(f"📊 <b>ОТЧЕТЫ КОМПАНИЙ</b>\n{news_item}", 
                                    parse_mode='HTML',
                                    content_type='news')
                await asyncio.sleep(4)
                has_news = True

        # Парсинг Positive Technologies
        pt_news = await self.parse_pt()
        if isinstance(pt_news, list) and pt_news:
            for news_item in pt_news:
                await self.tg.safe_send(f"📊 <b>ОТЧЕТЫ КОМПАНИЙ</b>\n{news_item}", 
                                    parse_mode='HTML',
                                    content_type='news')
                await asyncio.sleep(4)
                has_news = True
                
        # Парсинг Селигдар
        seligdar_news = await self.parse_seligdar()
        if isinstance(seligdar_news, list) and seligdar_news:
            for news_item in seligdar_news:
                await self.tg.safe_send(f"📊 <b>ОТЧЕТЫ КОМПАНИЙ</b>\n{news_item}", 
                                    parse_mode='HTML',
                                    content_type='news')
                await asyncio.sleep(4)
                has_news = True
                
        ozon_news = await self.parse_ozonpharm()
        if isinstance(ozon_news, list) and ozon_news:
            for news_item in ozon_news:
                await self.tg.safe_send(f"📊 <b>ОТЧЕТЫ КОМПАНИЙ</b>\n{news_item}", 
                                    parse_mode='HTML',
                                    content_type='news')
                await asyncio.sleep(4)
                has_news = True
        
        if not has_news:
            await self.tg.safe_send("ℹ️ Нет новых отчетов компаний",
                                content_type='news')

        return has_news