import logging
from pathlib import Path
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

class Screenshoter:
    @staticmethod
    async def take_screenshot(url: str, selector: str, save_path: str) -> bool:
        """Создание скриншота элемента страницы"""
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch()
                page = await browser.new_page()
                await page.goto(url, timeout=60000)
                element = await page.wait_for_selector(selector, timeout=30000)
                await element.screenshot(path=save_path)
                return True
        except Exception as e:
            logger.error(f"Screenshot failed: {str(e)}")
            return False