import requests
import logging
from config import Config

logger = logging.getLogger(__name__)

class YandexTranslator:
    def __init__(self):
        self.iam_token = None
        self.folder_id = Config.YANDEX_FOLDER_ID
        self.function_id = Config.YANDEX_FUNCTION_ID  # Добавьте в config.py

    async def renew_token(self) -> bool:
        """Обновление IAM токена"""
        try:
            if not hasattr(Config, 'YANDEX_FUNCTION_ID'):
                logger.error("YANDEX_FUNCTION_ID not configured")
                return False
                
            url = f'https://functions.yandexcloud.net/{Config.YANDEX_FUNCTION_ID}'
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'access_token' not in data:
                logger.error("No access_token in response")
                return False
                
            self.iam_token = data['access_token']
            logger.info("Yandex IAM token renewed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Token renewal failed: {str(e)}")
            return False

    async def translate(self, text: str, lang: str = "ru") -> str:
        """Перевод текста с обработкой ошибок"""
        if not text.strip():
            return text

        # Если токен не получен, пробуем обновить
        if not self.iam_token and not await self.renew_token():
            logger.warning("No IAM token available, returning original text")
            return text[:1000]  # Возвращаем обрезанный текст

        headers = {
            "Authorization": f"Bearer {self.iam_token}",
            "Content-Type": "application/json"
        }

        body = {
            "folderId": self.folder_id,
            "texts": [text[:1000]],  # Ограничение длины
            "targetLanguageCode": lang
        }

        try:
            response = requests.post(
                "https://translate.api.cloud.yandex.net/translate/v2/translate",
                json=body,
                headers=headers,
                timeout=10
            )

            # Если токен устарел, пробуем обновить и повторить
            if response.status_code == 401:
                if await self.renew_token():
                    headers["Authorization"] = f"Bearer {self.iam_token}"
                    response = requests.post(
                        "https://translate.api.cloud.yandex.net/translate/v2/translate",
                        json=body,
                        headers=headers,
                        timeout=10
                    )
                else:
                    return text[:1000]

            response.raise_for_status()
            return response.json()['translations'][0]['text']
            
        except Exception as e:
            logger.error(f"Translation error: {str(e)}")
            return text[:1000]  # Возвращаем обрезанный оригинал в случае ошибки