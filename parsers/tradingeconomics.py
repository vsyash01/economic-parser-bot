import logging
import hashlib
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright
from services.telegram_client import TelegramClient
from services.yandex_translator import YandexTranslator
from database import NewsDatabase
from aiogram import Bot, Dispatcher, F
from database import NewsDatabase

logger = logging.getLogger(__name__)

class TradingEconomicsParser:
    def __init__(self, dp: Dispatcher, db: NewsDatabase):  # Добавляем параметр db
        self.tg = TelegramClient(dp, db)  # Передаем оба параметра
        self.base_timeout = 45000  # 45 секунд для основных операций
        self.load_timeout = 90000  # 90 секунд для загрузки страницы
        self.db = db  # Сохраняем ссылку на БД для возможного будущего использования
        self.translator = YandexTranslator()

    async def get_chart_screenshot(self, page, news_id: str):
        """Скриншот графика с Trading Economics"""
        try:
            chart = page.locator('//div[@id="UpdatePanelChart"]')
            if await chart.count() > 0:
                path = f"te_chart_{news_id}.png"
                await chart.screenshot(path=path)
                return path
        except Exception as e:
            logger.warning(f"Chart screenshot failed: {str(e)[:100]}")
        return None

    async def parse_commodities_table(self, page):
        """Парсинг таблицы товарных активов (только топ-5)"""
        try:
            await page.wait_for_selector('.table.table-hover', timeout=30000)
            rows = await page.query_selector_all('table.table-hover tbody tr')
            
            commodities_data = []
            for row in rows[:5]:  # Берем только топ-5
                try:
                    # Получаем элементы
                    name_elem = await row.query_selector('.datatable-item-first b')
                    price_elem = await row.query_selector('td:nth-child(2)')
                    change_elem = await row.query_selector('td:nth-child(4)')
                    date_elem = await row.query_selector('td[id="date"]')
                    
                    if not all([name_elem, price_elem, change_elem]):
                        continue
                    
                    # Извлекаем текст
                    name = await name_elem.inner_text()
                    if name == 'Crude Oil':
                        name = 'WTI нефть'
                    elif name == 'Brent':
                        name = 'Brent нефть'
                    elif name == 'Natural gas':
                        name = 'Природный газ'
                    elif name == 'Gasoline':
                        name = 'Бензин'
                    elif name == 'Heating Oil':
                        name = 'Мазут'
                    elif name == 'Coal':
                        name = 'Уголь'
                    
                    price = await price_elem.inner_text()
                    change = await change_elem.inner_text()
                    date = await date_elem.inner_text() if date_elem else "N/A"
                    
                    # Очищаем и форматируем данные
                    price = f"${price}" if not price.startswith('$') else price
                    
                    # Добавляем "+" для положительных изменений
                    if not change.startswith('-') and not change.startswith('+'):
                        try:
                            change_float = float(change.replace('%', ''))
                            if change_float > 0:
                                change = f"+{change}"
                        except ValueError:
                            pass
                    
                    # Определяем цвет индикатора
                    emoji = "⚪"  # По умолчанию
                    try:
                        change_style = await change_elem.evaluate('el => el.getAttribute("style")')
                        if change_style:
                            if 'red' in change_style.lower() or 'darkred' in change_style.lower():
                                emoji = "🔴"
                            elif 'green' in change_style.lower() or 'darkgreen' in change_style.lower():
                                emoji = "🟢"
                        
                        # Дополнительная проверка по значению изменения
                        if change.startswith('-'):
                            emoji = "🔴"
                        elif change.startswith('+'):
                            emoji = "🟢"
                    except Exception as e:
                        logger.warning(f"Не удалось получить стиль изменения: {str(e)[:100]}")
                    
                    commodities_data.append(f"{emoji} {name}: {price} ({change}) | {date}")
                except Exception as e:
                    logger.warning(f"Ошибка парсинга строки: {str(e)[:100]}")
                    continue
            
            return commodities_data
        except Exception as e:
            logger.error(f"Ошибка парсинга таблицы товаров: {str(e)[:200]}")
            return []

    async def parse_crypto(self, page):
        """Парсинг криптовалют с исправленными селекторами"""
        try:
            await page.goto('https://tradingeconomics.com/crypto', 
                        timeout=120000)
            await asyncio.sleep(3)
            await page.wait_for_selector('.table.table-hover', timeout=30000)
        
            rows = await page.query_selector_all('table.table-hover tbody tr')
            crypto_data = []
        
            for row in rows[:10]:  # Берем топ-10
                try:
                    # Получаем текст из элементов
                    name = await (await row.query_selector('.datatable-item-first b')).inner_text()
                    price = await (await row.query_selector('td:nth-child(2)')).inner_text()
                    change = await (await row.query_selector('td:nth-child(4)')).inner_text()
                    date_elem = await (await row.query_selector('td[id="date"]')).inner_text()
                
                    # Форматирование данных
                    price = f"${price}" if not price.startswith('$') else price
                    if not change.startswith(('+', '-')):
                        try:
                            change_val = float(change.replace('%', ''))
                            change = f"+{change}" if change_val > 0 else change
                        except ValueError:
                            pass
                
                    # Определение цвета
                    emoji = "🔴" if change.startswith('-') else "🟢" if change.startswith('+') else "⚪"
                    crypto_data.append(f"{emoji} {name}: {price} ({change} | {date_elem})")
                except Exception as e:
                    logger.warning(f"Crypto row error: {str(e)[:100]}")
                    continue
        
            if crypto_data:
                await self.tg.safe_send("💰 <b>Топ-10 криптовалют:</b>\n" + "\n".join(crypto_data),
    content_type='crypto')
            else:
                await self.tg.safe_send("ℹ️ Данные по криптовалютам временно недоступны",
    content_type='crypto')
            
        except Exception as e:
            logger.error(f"Crypto parse failed: {str(e)}")
            await self.tg.safe_send("⚠️ Ошибка получения данных по криптовалютам",
    content_type='crypto')

    async def parse_news(self, page):
        """Парсинг новостей с переводом"""
        try:
            await page.goto('https://tradingeconomics.com/stream', timeout=60000)
            await page.wait_for_selector('.te-stream-item', timeout=30000)
            
            news_items = await page.query_selector_all('.te-stream-item')
            for item in news_items[:15]:
                try:
                    title_elem = await item.query_selector('.te-stream-title')
                    if not title_elem:
                        continue
                    
                    title = await title_elem.inner_text()
                    link = await title_elem.get_attribute('href')
                    if not link.startswith('http'):
                        link = f"https://tradingeconomics.com{link}"
                    
                    news_id = hashlib.md5(link.encode()).hexdigest()
                    if await self.db.is_news_exists(news_id):
                        continue
                    
                    text_elem = await item.query_selector('.te-stream-item-description')
                    text = await text_elem.inner_text() if text_elem else ""
                    
                    # Пробуем перевод, если не получится - отправляем оригинал
                    try:
                        translated = await self.translator.translate(f"{title}\n{text}")
                        if not translated:
                            translated = f"{title}\n{text}"
                    except Exception as e:
                        logger.warning(f"Translation failed: {str(e)[:100]}")
                        translated = f"{title}\n{text}"
                    
                    chart_path = None
                    try:
                        news_page = await page.context.new_page()
                        await news_page.goto(link, timeout=60000)
                        chart_path = await self.get_chart_screenshot(news_page, news_id)
                        await news_page.close()
                    except Exception as e:
                        logger.warning(f"News page error: {str(e)[:100]}")
                    
                    if translated:
                        await self.tg.safe_send(translated, image_path=chart_path,
                content_type='news')
                        await self.db.add_news(news_id, 'tradingeconomics', title, link)
                    
                    if chart_path and Path(chart_path).exists():
                        Path(chart_path).unlink()
                        
                except Exception as e:
                    logger.warning(f"News item error: {str(e)[:100]}")
                    continue
        except Exception as e:
            logger.error(f"News parse failed: {str(e)[:200]}")

    async def parse(self):
        """Основной метод парсинга"""
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                viewport={'width': 1200, 'height': 800}
            )
            page = await context.new_page()
            
            try:
                # Парсинг товарных активов
                await page.goto('https://tradingeconomics.com/commodities', timeout=60000)
                commodities = await self.parse_commodities_table(page)
                if commodities:
                    await self.tg.safe_send("🛢️ <b>Товарные активы:</b>\n" + "\n".join(commodities),
    content_type='commodities')
                
                # Остальные парсеры
                await self.parse_crypto(page)
                await self.parse_news(page)
                return True
            except Exception as e:
                logger.error(f"TE parsing failed: {str(e)}")
                return False
            finally:
                await browser.close()