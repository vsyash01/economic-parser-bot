import re
import logging
import hashlib
import requests
import asyncio
import feedparser
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from services.telegram_client import TelegramClient
from utils.html_formatter import HTMLFormatter
from database import NewsDatabase
from playwright.async_api import async_playwright
from aiogram import Bot, Dispatcher, F
from urllib.parse import urljoin


logger = logging.getLogger(__name__)

class RussianNewsParser:
    def __init__(self, dp: Dispatcher, db: NewsDatabase):  # Добавляем параметр db
        self.tg = TelegramClient(dp, db)  # Передаем оба параметра
        self.base_timeout = 45000  # 45 секунд для основных операций
        self.load_timeout = 90000  # 90 секунд для загрузки страницы
        self.db = db  # Сохраняем ссылку на БД для возможного будущего использования

        self.sources = {
#            'tass': self.parse_tass,
            'ria': self.parse_ria,
            'interfax': self.parse_interfax,
            'kommersant': self.parse_kommersant,
            '1prime': self.parse_1prime,
            'rb': self.parse_rb,
            'iz': self.parse_iz,
            'cbr': self.parse_cbr,
            'rbc': self.parse_rbc
        }

    def _russian_month_to_num(self, month_ru):
        months = {
            'января': '01', 'февраля': '02', 'марта': '03',
            'апреля': '04', 'мая': '05', 'июня': '06',
            'июля': '07', 'августа': '08', 'сентября': '09',
            'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        return months.get(month_ru.lower(), '01')
    
    async def parse_tass(self):
        """Парсинг новостей ТАСС"""
        news = []
        try:
            response = requests.get('https://tass.ru/ekonomika', timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for card in soup.select('div[class*="card"], div[class*="article"]')[:15]:
                try:
                    title_elem = card.select_one('span[class*="title"], h2, h3')
                    link_elem = card.find('a', href=True) or card.find_parent('a', href=True)
                    
                    if not title_elem or not link_elem:
                        continue
                        
                    title = title_elem.get_text(strip=True)
                    link = link_elem['href']
                    if not link.startswith('http'):
                        link = f"https://tass.ru{link}"
                        #print(f'link = {link}')
                    
                    news_id = hashlib.md5(link.encode()).hexdigest()
                    if await self.db.is_news_exists(news_id):
                        continue
                    
                    time_elem = card.select_one('div[class*="time"], time, span[class*="date"]')
                    time_text = time_elem.get_text(strip=True) if time_elem else ""
                    
                    if 'минут' in time_text:
                        mins = int(''.join(filter(str.isdigit, time_text)))
                        date_str = (datetime.now() - timedelta(minutes=mins)).strftime('%d.%m.%Y %H:%M')
                    elif 'час' in time_text:
                        hours = int(''.join(filter(str.isdigit, time_text)))
                        date_str = (datetime.now() - timedelta(hours=hours)).strftime('%d.%m.%Y %H:%M')
                    else:
                        date_str = time_text
                    
                    news_item = f"{title} ({date_str}) <a href='{link}'>— ТАСС</a>"
                    news.append(news_item)
                    await self.db.add_news(news_id, 'tass', title, link)
                except Exception as e:
                    logger.warning(f"TASS card error: {str(e)[:100]}")
                    continue
                    
            return news
        except Exception as e:
            logger.error(f"TASS parse failed: {str(e)[:200]}")
            return []

    async def parse_ria(self):
        """Парсинг новостей РИА"""
        news = []
        try:
            response = requests.get('https://ria.ru/economy/', timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')

            for item in soup.select('div.list-item')[:15]:
                try:
                    title_elem = item.select_one('a.list-item__title')
                    if not title_elem:
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    link = title_elem['href']
                    if not link.startswith('http'):
                        link = f"https://ria.ru{link}"
                
                    # Получаем блок с датой
                    date_elem = item.select_one('div.list-item__info-item[data-type="date"]')
                    date_text = date_elem.get_text(strip=True) if date_elem else ""
                
                    # Преобразуем "Вчера"/"Сегодня" в конкретные даты
                    now = datetime.now()
                    if "Вчера" in date_text:
                        date_str = (now - timedelta(days=1)).strftime('%d.%m.%Y') + date_text.replace("Вчера", "")
                    elif "Сегодня" in date_text:
                        date_str = now.strftime('%d.%m.%Y') + date_text.replace("Сегодня", "")
                    else:
                        date_str = date_text
                
                    news_id = hashlib.md5(link.encode()).hexdigest()
                    #print(f'link = {link}\nnews id = {news_id}')
                    if await self.db.is_news_exists(news_id):
                        continue
                
                    time_part = f" ({date_str})" if date_str else ""
                    news_item = f"{title}{time_part} <a href='{link}'>— РИА</a>"
                    news.append(news_item)
                    await self.db.add_news(news_id, 'ria', title, link)
                
                except Exception as e:
                    logger.warning(f"RIA item error: {str(e)[:100]}")
                    continue
    
        except Exception as e:
            logger.error(f"RIA parse failed: {str(e)[:200]}")
    
        return news
        
    async def parse_interfax(self):
        """Парсинг новостей Interfax"""
        news = []
        try:
            response = requests.get('https://www.interfax.ru/business/', timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Обрабатываем все новостные блоки (обычные и фото)
            for item in soup.select('div.timeline__group > div, div.timeline__photo, div.timeline__text'):
                try:
                    # Общий поиск элементов для всех типов новостей
                    time_elem = item.find('time')
                    title_elem = item.find('a') if not item.select_one('a.timeline_link') else None
                    
                    if not title_elem or not time_elem:
                        continue
                    
                    title = title_elem.get('title') or title_elem.get_text(strip=True)
                    link = title_elem['href']
                    if not link.startswith('http'):
                        link = f"https://www.interfax.ru{link}"
                    
                    # Берем время из атрибута datetime или текста
                    time_text = time_elem.get('datetime', '').split('T')[1][:5] if time_elem.get('datetime') else time_elem.get_text(strip=True)
                    
                    news_id = hashlib.md5(link.encode()).hexdigest()
                    if await self.db.is_news_exists(news_id):
                        continue
                    
                    news_item = f"{title} ({time_text}) <a href='{link}'>— Interfax</a>"
                    news.append(news_item)
                    await self.db.add_news(news_id, 'interfax', title, link)
                except Exception as e:
                    logger.warning(f"Interfax item error: {str(e)[:100]}")
                    continue
        
            return news
        except Exception as e:
            logger.error(f"Interfax parse failed: {str(e)[:200]}")
            return []

    async def parse_kommersant(self):
        """Парсинг новостей Коммерсантъ"""
        news = []
        try:
            logger.info("Starting Kommersant parser")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get('https://www.kommersant.ru/rubric/3', headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Основной селектор статей - охватывает все варианты
            articles = soup.select('article.rubric_lenta__item, article.uho, div.rubric_lenta__item')
            logger.info(f"Found {len(articles)} potential news articles")
            
            for article in articles[:15]:
                try:
                    # Универсальные селекторы заголовка
                    title_elem = (
                        article.select_one('h2 a.uho__link') or
                        article.select_one('h2.rubric_lenta__item_name a') or
                        article.select_one('h2 a') or
                        article.find('a', {'data-article-title': True}) or
                        article.select_one('a.list-item__title')  # Дополнительный вариант
                    )
                    
                    if not title_elem:
                        logger.debug(f"Skipping article - no title found. Article HTML:\n{str(article)[:300]}...")
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    link = title_elem.get('href', '')
                    
                    if not link:
                        logger.debug(f"Skipping article - no link in title element: {title[:50]}...")
                        continue
                        
                    if not link.startswith('http'):
                        link = f"https://www.kommersant.ru{link}"
                    
                    # Универсальные селекторы даты
                    date_elem = (
                        article.select_one('p.uho__tag.rubric_lenta__item_tag:not(.hide_mobile)') or
                        article.select_one('p.uho__tag.hide_desktop') or
                        article.select_one('p.rubric_lenta__item_tag') or
                        article.select_one('time') or
                        article.select_one('div.article__time')  # Дополнительный вариант
                    )
                    
                    date_text = date_elem.get_text(strip=True) if date_elem else ""
                    logger.debug(f"Processing article: {title[:50]}... | Date raw: '{date_text}'")
                    
                    # Улучшенная обработка даты
                    if date_text:
                        if ',' in date_text:  # Формат "24.04.2025, 08:42"
                            date_text = date_text.split(',')[1].strip()
                        elif 'вчера' in date_text.lower() or 'сегодня' in date_text.lower():
                            # Обработка относительных дат
                            date_text = datetime.now().strftime('%H:%M')
                    
                    news_id = hashlib.md5(link.encode()).hexdigest()
                    
                    if await self.db.is_news_exists(news_id):
                        logger.debug(f"Article already in DB: {title[:50]}...")
                        continue
                    
                    news_item = f"{title} ({date_text}) <a href='{link}'>— Ъ</a>"
                    news.append(news_item)
                    await self.db.add_news(news_id, 'kommersant', title, link)
                    
                except Exception as e:
                    logger.error(f"Error processing article: {str(e)}\nArticle snippet:\n{str(article)[:300]}...")
                    continue

        except Exception as e:
            logger.error(f"Kommersant parse failed: {str(e)}")
        
        logger.info(f"Kommersant parser finished. Found {len(news)} new articles")
        return news

    async def parse_1prime(self):
        """Парсинг новостей с 1prime.ru (ПРАЙМ)"""
        news = []
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch()
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = await context.new_page()
                
                await page.goto('https://1prime.ru/simple_ROSSIJA+state_regulation/', timeout=60000)
                
                # Пытаемся закрыть дисклеймер
                try:
                    close_button = page.locator('use[xlink:href="#icon-close"]').first
                    await close_button.click(timeout=5000)
                    logger.info("Дисклеймер 1prime.ru закрыт")
                except Exception as e:
                    logger.warning(f"Дисклеймера на 1prime.ru нет или не удалось закрыть: {str(e)[:200]}")
                
                # Ждем загрузки контента
                await page.wait_for_selector('div.list.list-tags div.list-item', timeout=15000)
                
                # Получаем HTML после возможного закрытия дисклеймера
                html = await page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                news_list = soup.select('div.list.list-tags div.list-item')
                
                for item in news_list[:15]:
                    try:
                        title_elem = item.select_one('a.list-item__title')
                        time_elem = item.select_one('div.list-item__date')
                        
                        if not title_elem or not time_elem:
                            continue
                        
                        title = title_elem.get_text(strip=True)
                        #print(title)
                        time = time_elem.get_text(strip=True)
                        #print(f'{time}')
                        link = "https://1prime.ru" + title_elem['href']
                        #print(f'link = {link}')
                        
                        news_id = hashlib.md5(link.encode()).hexdigest()
                        #print(f'link = {link}\nnews id = {news_id}')
                        if await self.db.is_news_exists(news_id):
                            continue
                            
                        news_item = f"{title} ({time}) <a href='{link}'>— ПРАЙМ</a>"
                        #print(news_item)
                        news.append(news_item)
                        #print(f'news = {news}')
                        await self.db.add_news(news_id, '1prime', title, link)
                    except Exception as e:
                        logger.warning(f"Ошибка обработки новости 1prime: {str(e)[:100]}")
                        continue
                
                await browser.close()
        
        except Exception as e:
            logger.error(f"Ошибка парсинга 1prime: {str(e)[:200]}")
        return news
        #return news if news else ["ℹ️ Не удалось загрузить новости ПРАЙМ"]

    async def parse_rb(self):
        """Парсинг новостей RB.RU (финансы, сделки, ВВП, бизнес)"""
        news = []
        base_url = "https://rb.ru"
        sections = [
            "/tag/finance/",
            "/tag/deal/", 
            "/tag/vvp/",
            "/tag/business/"
        ]
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            
            for section in sections:
                try:
                    url = f"{base_url}{section}"
                    response = requests.get(url, headers=headers, timeout=10)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    news_blocks = soup.select('div.news-item')[:15]
                    
                    for block in news_blocks:
                        try:
                            title_elem = block.select_one('a.news-item__title')
                            if not title_elem:
                                continue
                            title = title_elem.get_text(strip=True)
                            
                            link = title_elem['href']
                            if not link.startswith('http'):
                                link = f"{base_url}{link}"
                                #print(f'link = {link}')
                            
                            time_elem = block.select_one('time.news-item__date')
                            time_text = time_elem.get_text(strip=True) if time_elem else ""
                            
                            # Обработка даты (пример: "21 апреля 17:30")
                            date_str = ""
                            if time_text:
                                try:
                                    clean_text = time_text.replace("г.", "").strip()
                                    months = {
                                        'января': '01', 'февраля': '02', 'марта': '03',
                                        'апреля': '04', 'мая': '05', 'июня': '06',
                                        'июля': '07', 'августа': '08', 'сентября': '09',
                                        'октября': '10', 'ноября': '11', 'декабря': '12'
                                    }
                                    
                                    parts = clean_text.split()
                                    day = parts[0].zfill(2)
                                    month = months.get(parts[1], '01')
                                    time_part = parts[2] if len(parts) > 2 else "00:00"
                                    
                                    current_year = datetime.now().year
                                    date_str = f"{day}.{month}.{current_year} {time_part}"
                                except Exception as e:
                                    logger.warning(f"Ошибка обработки даты RB.RU: {str(e)[:100]}")
                            
                            news_id = hashlib.md5(link.encode()).hexdigest()
                            #print(f'link = {link}\nnews id = {news_id}')
                            if await self.db.is_news_exists(news_id):
                                continue
                            
                            date_part = f" ({date_str})" if date_str else ""
                            source_link = f'<a href="{link}">— RB.RU</a>'
                            news_item = f"{title}{date_part} {source_link}"
                            news.append(news_item)
                            await self.db.add_news(news_id, 'rb', title, link)
                            
                        except Exception as e:
                            logger.warning(f"Ошибка обработки новости RB.RU: {str(e)[:100]}")
                            continue
                    
                except Exception as e:
                    logger.error(f"Ошибка парсинга RB.RU {section}: {str(e)[:200]}")
                    continue
            
            # Удаляем дубликаты
            seen = set()
            unique_news = []
            for item in news:
                if item not in seen:
                    seen.add(item)
                    unique_news.append(item)
            
            return unique_news[:30]
        
        except Exception as e:
            logger.error(f"Ошибка парсинга RB.RU: {str(e)[:200]}")
            return []

    async def parse_iz(self):
        news = []
        base_url = "https://iz.ru"
        now = datetime.now()
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0...'}
            
            for section in ['/rubric/ekonomika', '/tag/rubl', '/tag/neft', '/tag/dollar']:
                url = f"{base_url}{section}"
                response = requests.get(url, headers=headers, timeout=15)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Универсальный поиск новостных блоков
                news_blocks = (
                    soup.select('div.node__cart__item.show_views_and_comments') 
                    if 'rubric/ekonomika' in url 
                    else soup.select('div.tag-materials-item__box')
                )
                
                for block in news_blocks[:30]:
                    try:
                        # Извлечение заголовка и ссылки (как в предыдущем коде)
                        title = (block.select_one('h3.tag-materials-item__title') or 
                                block.select_one('div.node__cart__item__inside__info__title span')).get_text(strip=True)
                        
                        link_elem = (block.find('a', class_='tag-materials-item') or 
                                    block.find('a', class_='node__cart__item__inside'))
                        link = urljoin(base_url, link_elem['href']) if link_elem else "#"
                        
                        # Обработка даты
                        date_str = ""
                        time_elem = block.select_one('time') or block.select_one('div.tag-materials-item__date')
                        
                        if time_elem:
                            # Парсинг машинного формата из атрибута datetime (если есть)
                            if time_elem.get('datetime'):
                                pub_date = datetime.strptime(
                                    time_elem['datetime'].split('T')[0], 
                                    '%Y-%m-%d'
                                )
                                time_part = time_elem['datetime'].split('T')[1][:5]
                            else:
                                # Парсинг человекочитаемого формата
                                date_text = time_elem.get_text(strip=True)
                                try:
                                    if ',' in date_text:  # Формат "24 апреля 2025, 08:00"
                                        date_part, time_part = date_text.split(',')
                                        day, month_ru, year = date_part.strip().split()
                                        month = self._russian_month_to_num(month_ru)
                                        pub_date = datetime.strptime(
                                            f"{day} {month} {year}", 
                                            "%d %m %Y"
                                        )
                                    else:  # Альтернативный формат без времени
                                        day, month_ru, year = date_text.strip().split()[:3]
                                        month = self._russian_month_to_num(month_ru)
                                        pub_date = datetime.strptime(
                                            f"{day} {month} {year}", 
                                            "%d %m %Y"
                                        )
                                        time_part = ""
                                except ValueError as e:
                                    logger.warning(f"Не удалось распарсить дату: {date_text} | {str(e)}")
                                    continue
                            
                            # Форматирование вывода
                            if pub_date.date() == now.date():
                                date_str = time_part if time_part else ""
                            else:
                                date_str = f"{pub_date.strftime('%d.%m.%Y')} {time_part}" if time_part else pub_date.strftime('%d.%m.%Y')
                        
                        news_id = hashlib.md5(link.encode()).hexdigest()
                    
                        if await self.db.is_news_exists(news_id):
                            logger.debug(f"Article exists in DB: {title[:50]}...")
                            continue
                        
                        
                        # Формирование итоговой строки
                        time_part = f" ({date_str})" if date_str else ""
                        news_item = f"{title}{time_part} <a href='{link}'>— Известия</a>"
                        news.append(news_item)
                        await self.db.add_news(news_id, 'iz', title, link)
                        
                    except Exception as e:
                        logger.warning(f"Ошибка обработки новости: {str(e)[:200]}")
                        continue
        
        except Exception as e:
            logger.error(f"Ошибка парсинга Известий: {str(e)[:200]}")
        
        return news

    async def parse_cbr(self):
        """Парсинг новостей ЦБ РФ"""
        try:
            feed = feedparser.parse("https://cbr.ru/rss/eventrss")
            news = []
            for entry in feed.entries[:15]:  # Берем 5 последних новостей
                try:
                    date = datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %z').strftime('%d.%m.%Y %H:%M')
                    news_id = hashlib.md5(entry.link.encode()).hexdigest()
                    #print(f'link = {entry.link}\nnews id = {news_id}')
                    #print(f'link = {entry.link}')
                    
                    if await self.db.is_news_exists(news_id):
                        continue
                    
                    source_link = f'<a href="{entry.link}">— ЦБ РФ</a>'
                    news_item = f"{entry.title} ({date}) {source_link}"
                    news.append(news_item)
                    await self.db.add_news(news_id, 'cbr', entry.title, entry.link)
                except Exception as e:
                    logger.warning(f"Ошибка обработки новости ЦБ РФ: {str(e)[:100]}")
                    continue
            
            return news
        except Exception as e:
            logger.error(f"Ошибка парсинга ЦБ РФ: {str(e)[:200]}")
            return []

    async def parse_rbc(self):
        """Парсинг новостей РБК"""
        news = []
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response = requests.get('https://www.rbc.ru/quote', headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            news_blocks = soup.select('div.q-item__wrap')[:15]
            
            for block in news_blocks:
                try:
                    title_elem = block.select_one('span.q-item__title')
                    if not title_elem:
                        continue
                    title = title_elem.get_text(strip=True)
                    
                    link_elem = block.find('a', class_='q-item__link')
                    if not link_elem or not link_elem.get('href'):
                        continue
                    link = link_elem['href']
                    if not link.startswith('http'):
                        link = f"https://www.rbc.ru{link}"
                        #print(f'link = {link}')
                        
                    
                    time_elem = block.select_one('span.q-item__date__text')
                    time_text = ''
                    if time_elem:
                        publisher = time_elem.select_one('span.q-item__date__publisher')
                        if publisher:
                            publisher.extract()
                        time_text = time_elem.get_text(strip=True).replace(',', '').strip()
                    
                    # Форматируем дату/время
                    date_str = ''
                    if time_text:
                        now = datetime.now()
                        if ':' in time_text:
                            date_str = f"{now.strftime('%d.%m.%Y')} {time_text}"
                        elif 'мин' in time_text:
                            mins = int(''.join(filter(str.isdigit, time_text)))
                            date_str = (now - timedelta(minutes=mins)).strftime('%d.%m.%Y %H:%M')
                        elif 'час' in time_text:
                            hours = int(''.join(filter(str.isdigit, time_text)))
                            date_str = (now - timedelta(hours=hours)).strftime('%d.%m.%Y %H:%M')
                    
                    news_id = hashlib.md5(link.encode()).hexdigest()
                    #print(f'link = {link}\nnews id = {news_id}')
                    if await self.db.is_news_exists(news_id):
                        continue
                    
                    date_part = f" ({date_str})" if date_str else ""
                    source_link = f'<a href="{link}">— РБК</a>'
                    news_item = f"{title}{date_part} {source_link}"
                    news.append(news_item)
                    await self.db.add_news(news_id, 'rbc', title, link)
                    
                except Exception as e:
                    logger.warning(f"Ошибка обработки новости РБК: {str(e)[:100]}")
                    continue
            
            return news
        
        except Exception as e:
            logger.error(f"Ошибка парсинга РБК: {str(e)[:200]}")
            return []

    async def parse(self):
        """Основной метод парсинга всех источников"""
        all_news = []
        for source_name, parser in self.sources.items():
            try:
                news = await parser()
                if isinstance(news, list):
                    all_news.extend(news)
                logger.info(f"Parsed {len(news)} news from {source_name}")
            except Exception as e:
                logger.error(f"Failed to parse {source_name}: {str(e)}")

        if all_news:
            formatted = HTMLFormatter.format_news_with_priority(all_news)
            await self.tg.safe_send(f"📌 <b>ЭКОНОМИЧЕСКИЕ НОВОСТИ</b>\n{formatted}", parse_mode='HTML',
                content_type='news')
        else:
            await self.tg.safe_send("ℹ️ Нет новых экономических новостей",
                content_type='news')

        return bool(all_news)