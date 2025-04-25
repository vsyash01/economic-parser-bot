import logging
from playwright.async_api import async_playwright
from services.telegram_client import TelegramClient
from aiogram import Bot, Dispatcher, F
from database import NewsDatabase

logger = logging.getLogger(__name__)

class DividendsParser:
    def __init__(self, dp: Dispatcher, db: NewsDatabase):  # Добавляем параметр db
        self.tg = TelegramClient(dp, db)  # Передаем оба параметра
        self.base_timeout = 45000  # 45 секунд для основных операций
        self.load_timeout = 90000  # 90 секунд для загрузки страницы
        self.db = db  # Сохраняем ссылку на БД для возможного будущего использования

    async def parse(self):
        """Парсинг дивидендов с SmartLab"""
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch()
                page = await browser.new_page()
                
                await page.goto('https://smart-lab.ru/dividends/', timeout=60000)
                await page.wait_for_selector('table.simple-little-table', timeout=15000)
                
                rows = await page.query_selector_all('table.simple-little-table tbody tr.dividend_approved')
                dividends = []
                
                for row in rows[:10]:  # Только топ-10
                    try:
                        cells = await row.query_selector_all('td')
                        if len(cells) >= 10:
                            company = (await cells[0].inner_text()).strip()
                            ticker = (await cells[1].inner_text()).strip()
                            amount = (await cells[3].inner_text()).strip()
                            yield_pct = (await cells[4].inner_text()).strip('%')
                            payment_date = (await cells[8].inner_text()).strip()
                            
                            div_str = (
                                f"💰 {company} ({ticker}): {amount} руб. "
                                f"(доходность {yield_pct}%), "
                                f"дата выплаты: {payment_date or 'не указана'}"
                            )
                            dividends.append(div_str)
                    except Exception as e:
                        logger.warning(f"Dividend row error: {str(e)[:100]}")
                        continue
                
                if dividends:
                    await self.tg.safe_send("💵 <b>Ближайшие дивиденды:</b>\n" + "\n".join(dividends),
    content_type='dividends')
                else:
                    await self.tg.safe_send("ℹ️ Нет данных о дивидендах",
    content_type='dividends')
                
                return True
        except Exception as e:
            logger.error(f"Dividend parse failed: {str(e)}")
            await self.tg.safe_send("⚠️ Ошибка получения данных по дивидендам",
    content_type='dividends')
            return False