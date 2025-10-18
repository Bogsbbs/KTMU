import os
import logging
from bot import ScheduleBot

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Получаем порт из переменной окружения (для Scalingo)
port = int(os.environ.get("PORT", 8080))

def main():
    """Запуск бота"""
    logger.info(f"🚀 Starting bot on port {port}")
    
    bot = ScheduleBot()
    bot.run()

if __name__ == '__main__':
    main()
