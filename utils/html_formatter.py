from typing import List
import logging

logger = logging.getLogger(__name__)

class HTMLFormatter:
    @staticmethod
    def format_news_with_priority(news_items: List[str]) -> str:
        """Форматирование новостей с приоритетами"""
        priority_keywords = [
            'ЦБ', 'ставка', 'инфляция', 'Минфин', 'санкции',
            'нефть', 'газ', 'рубль', 'доллар', 'евро', 'биржа',
            'ВВП', 'экономика', 'кризис', 'индекс', 'акции'
        ]

        blacklist = [
            'Зеленский', 'Украина', 'спорт', 'футбол',
            'теннис', 'COVID', 'коронавирус', 'вакцина'
        ]

        high_priority = []
        medium_priority = []

        for item in news_items:
            lower_item = item.lower()
            
            if any(bad.lower() in lower_item for bad in blacklist):
                continue
                
            if any(keyword.lower() in lower_item for keyword in priority_keywords):
                high_priority.append(item)
            else:
                medium_priority.append(item)

        message_parts = []
        if high_priority:
            message_parts.append("🔴 <b>ВАЖНЫЕ НОВОСТИ</b>")
            message_parts.extend(f"{i}. {item}" for i, item in enumerate(high_priority[:15], 1))
        
        if medium_priority:
            message_parts.append("\n🔵 <b>ДРУГИЕ НОВОСТИ</b>")
            message_parts.extend(f"• {item}" for item in medium_priority[:20])

        return "\n".join(message_parts) if message_parts else "ℹ️ Нет новых значимых новостей"