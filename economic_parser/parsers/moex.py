import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright
from services.telegram_client import TelegramClient
from config import Config
from aiogram import Bot, Dispatcher, F
from database import NewsDatabase

logger = logging.getLogger(__name__)

class MOEXParser:
    def __init__(self, dp: Dispatcher, db: NewsDatabase):  # Добавляем параметр db
        self.tg = TelegramClient(dp, db)  # Передаем оба параметра
        self.base_timeout = 45000  # 45 секунд для основных операций
        self.load_timeout = 90000  # 90 секунд для загрузки страницы
        self.db = db  # Сохраняем ссылку на БД для возможного будущего использования

    async def handle_disclaimer(self, page):
        """Обработка дисклеймера MOEX с несколькими вариантами селекторов"""
        try:
            accept_buttons = [
                'button:has-text("Принимаю")',
                '.btn2.btn2-primary',
                'button[type="submit"]:has-text("Принять")',
                'text=/Принять|Согласен|Agree/i'
            ]
            
            for selector in accept_buttons:
                try:
                    if await page.locator(selector).count() > 0:
                        await page.click(selector, timeout=15000)
                        logger.info("MOEX disclaimer accepted")
                        await asyncio.sleep(1)  # Даем время для применения изменений
                        return True
                except Exception as e:
                    logger.debug(f"Disclaimer button not found with selector: {selector}")
                    continue
            
            logger.info("No disclaimer found or already accepted")
            return False
            
        except Exception as e:
            logger.warning(f"Error handling disclaimer: {str(e)[:200]}")
            return False

    async def parse_indexes(self, page):
        """Парсинг основных индексов MOEX с переключением страниц"""
        indexes = []
        try:
            # Получаем дату торгов из заголовка
            trade_date = await page.locator('header h2:first-child').text_content()
            trade_date = trade_date.replace('Ход торгов,', '').strip()
            
            # Выбираем группу "Индексы" и ждем загрузки
            await page.select_option('#securitygroups', value='12', timeout=self.base_timeout)
            await self._click_with_retry(page, '.MarketDataNewGroup_submitButton_RV0VB')
            
            # Ждем появления таблицы
            await page.wait_for_selector('.ui-table', state='visible', timeout=self.base_timeout)
            await asyncio.sleep(2)  # Дополнительное время для стабилизации
            
            # Собираем данные по индексам
            imoex = await self._get_index_data(page, 'IMOEX', 'Индекс МосБиржи', trade_date)
            rts = await self._get_index_data(page, 'RTSI', 'Индекс РТС', trade_date)
            rgbi = await self._get_index_data(page, 'RGBI', 'Индекс Мосбиржи гос обл RGBI', trade_date)
            
            if imoex:
                indexes.append(imoex)
            else:
                indexes.append(f"ℹ️ Индекс МосБиржи (IMOEX): данные недоступны ({trade_date})")
                
            if rts:
                indexes.append(rts)
            else:
                indexes.append(f"ℹ️ Индекс РТС (RTSI): данные недоступны ({trade_date})")
                
            if rgbi:
                indexes.append(rgbi)
            else:
                indexes.append(f"ℹ️ Индекс гос. облигаций (RGBI): данные недоступны ({trade_date})")
                
            return indexes, trade_date
            
        except Exception as e:
            logger.error(f"Error parsing indexes: {str(e)[:200]}")
            return [
                "ℹ️ Индекс МосБиржи (IMOEX): данные недоступны",
                "ℹ️ Индекс РТС (RTSI): данные недоступны",
                "ℹ️ Индекс гос. облигаций (RGBI): данные недоступны"
            ], "дата неизвестна"
            
        except Exception as e:
            logger.error(f"Error parsing indexes: {str(e)[:200]}")
            return [
                "ℹ️ Индекс МосБиржи (IMOEX): данные недоступны",
                "ℹ️ Индекс РТС (RTSI): данные недоступны",
                "ℹ️ Индекс гос. облигаций (RGBI): данные недоступны"
            ], "дата неизвестна"

    async def _get_index_data(self, page, ticker, name, trade_date):
        """Поиск данных конкретного индекса с перебором страниц"""
        try:
            max_pages_to_check = 11
            pages_checked = 1

            # Проверяем текущую страницу
            result = await self._check_current_page_for_index(page, ticker, name, trade_date)
            if result:
                return result
    
            # Перебираем следующие страницы
            while pages_checked < max_pages_to_check:
                try:
                    next_btn = page.locator('button.UiPaginationButton_buttonNew_KSmaR >> text="Вперед ›"')
                    if await next_btn.is_enabled():
                        await next_btn.click()
                        await asyncio.sleep(2)
                        await page.wait_for_selector('.ui-table', timeout=10000)
                        pages_checked += 1
                
                        result = await self._check_current_page_for_index(page, ticker, name, trade_date)
                        if result:
                            return result
                    else:
                        break
                except Exception as e:
                    logger.warning(f"Ошибка перехода на следующую страницу: {str(e)[:100]}")
                    break
    
            # Возвращаемся на первую страницу
            while pages_checked > 1:
                try:
                    prev_btn = page.locator('button.UiPaginationButton_buttonNew_KSmaR >> text="‹ Назад"')
                    if await prev_btn.is_enabled():
                        await prev_btn.click()
                        await asyncio.sleep(2)
                        pages_checked -= 1
                    else:
                        break
                except Exception as e:
                    logger.warning(f"Ошибка возврата на предыдущую страницу: {str(e)[:100]}")
                    break
                
            return None

        except Exception as e:
            logger.warning(f"Error getting index {ticker}: {str(e)[:100]}")
            return None

    async def _check_current_page_for_index(self, page, ticker, name, trade_date):
        """Проверка текущей страницы на наличие нужного индекса"""
        try:
            rows = await page.query_selector_all('.ui-table-row.-interactive')
            for row in rows:
                try:
                    row_text = await row.inner_text()
                    parts = [p.strip() for p in row_text.split('\n') if p.strip()]

                    if len(parts) >= 4:
                        row_ticker = parts[0].replace('\xa0', ' ').strip()
                        row_name = parts[1].replace('\xa0', ' ').strip()

                        if row_ticker == ticker and row_name == name:
                            price = parts[2].replace('.', ',')
                            change = parts[3].replace('.', ',')
                            time_elem = await row.query_selector('td:last-child div')
                            time = await time_elem.text_content() if time_elem else "время неизвестно"
                            emoji = "🟢" if '+' in change else "🔴" if '-' in change else "⚪"
                            return f"{emoji} {name} ({ticker}): {price} {change} | {time}"
                except:
                    continue
            return None
        except Exception as e:
            logger.warning(f"Error checking page for index {ticker}: {str(e)[:100]}")
            return None

    async def parse_stocks(self, page, trade_date):
        """Парсинг топовых акций MOEX"""
        stocks = []
        try:
            # Переключаемся на акции
            await page.select_option('#securitygroups', value='4', timeout=self.base_timeout)
            await self._click_with_retry(page, '.MarketDataNewGroup_submitButton_RV0VB')
            
            # Ждем загрузки таблицы
            await page.wait_for_selector('.ui-table', state='visible', timeout=self.base_timeout)
            await asyncio.sleep(2)
            
            # Парсим первые 15 акций (используем оригинальные локаторы)
            rows = await page.query_selector_all('.ui-table-row.-interactive')
            for row in rows[:15]:
                try:
                    # Получаем все данные из строки
                    cells = await row.query_selector_all('td')
                    if len(cells) < 9:  # Проверяем, что есть все нужные столбцы
                        continue
                    
                    # Тикер из первого столбца
                    ticker_elem = await cells[0].query_selector('a')
                    ticker = (await ticker_elem.text_content()).strip() if ticker_elem else ""
                    
                    # Название из второго столбца
                    name = (await cells[1].text_content()).strip()
                    
                    # Цена из третьего столбца
                    price = (await cells[2].text_content()).strip().replace('.', ',')
                    
                    # Изменение из четвертого столбца
                    #change_elem = cells[3]
                    change_elem = await cells[3].query_selector('.PercentValue_cell_2te0M')  # Ищем внутренний div
                    if change_elem:
                        change_class = await change_elem.get_attribute('class')
                        logger.debug(f"Found change classes: {change_class}")
                    else:
                        change_class = ""
                        logger.warning("Change element not found")
                    change = (await change_elem.text_content()).strip()
                    #change_class = await change_elem.get_attribute('class')
                    #print(f'change_class = {change_class}\n')
                    # Время из последнего столбца
                    time = (await cells[-1].text_content()).strip()
                    
                    # Определяем цвет изменения
                    if 'PercentValue_cell_modUp' in change_class:
                        emoji = "🟢"
                    elif 'PercentValue_cell_modDown' in change_class:
                        emoji = "🔴"
                    else:
                        emoji = "⚪"
                        change = f"{change.replace('%', '').strip()}%"
                    stocks.append(f"{emoji} {name} ({ticker}): {price} {change} | {time}")
                    
                except Exception as e:
                    logger.warning(f"Error parsing stock row: {str(e)[:100]}")
                    continue
                    
            return stocks if stocks else [f"ℹ️ Котировки акций временно недоступны"]
            
        except Exception as e:
            logger.error(f"Error parsing stocks: {str(e)[:200]}")
            return [f"ℹ️ Ошибка при получении данных по акциям"]

    async def _click_with_retry(self, page, selector, attempts=3, delay=2):
        """Повторные попытки клика с задержкой"""
        for attempt in range(attempts):
            try:
                await page.click(selector, timeout=15000)
                await asyncio.sleep(1)  # Короткая пауза после клика
                return True
            except Exception as e:
                if attempt == attempts - 1:
                    raise
                logger.debug(f"Click attempt {attempt + 1} failed, retrying...")
                await asyncio.sleep(delay)
        return False

    async def parse(self):
        """Основной метод парсинга данных с MOEX"""
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                timeout=self.load_timeout
            )
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()
            
            try:
                # Загрузка основной страницы
                await page.goto('https://www.moex.com/ru/marketdata/', timeout=self.load_timeout)
                await self.handle_disclaimer(page)
                
                # Парсим индексы и получаем дату торгов
                indexes, trade_date = await self.parse_indexes(page)
                
                # Парсим акции
                stocks = await self.parse_stocks(page, trade_date)
                
                # Формируем сообщение с заголовком
                message = f"📊 <b>Рынок акций и индексы (Ход торгов, {trade_date}):</b>\n" + "\n".join(indexes)
                if stocks:
                    message += f"\n\n🏛 <b>Топ-15 акций (Ход торгов, {trade_date}):</b>\n" + "\n".join(stocks)
                
                await self.tg.safe_send(message, content_type='stocks')
                return True
                
            except Exception as e:
                logger.error(f"MOEX parsing failed: {str(e)}")
                await self.tg.safe_send("⚠️ Ошибка получения данных с MOEX", content_type='stocks')
                return False
                
            finally:
                await context.close()
                await browser.close()