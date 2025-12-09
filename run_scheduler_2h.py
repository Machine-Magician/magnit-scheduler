import asyncio
import sys
import os
from dotenv import load_dotenv
from pathlib import Path


# Загружаем .env с настройкой интервала
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

# Можно переопределить интервал здесь
os.environ["CHECK_INTERVAL_MINUTES"] = "120"  # 2 часа

from src.services.scheduler_service import main as scheduler_main

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    print(f"🔧 Настройка: проверка БД каждые {os.getenv('CHECK_INTERVAL_MINUTES')} минут")
    asyncio.run(scheduler_main())