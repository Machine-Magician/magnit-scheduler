# run_scheduler.py (в корне проекта)
import asyncio
import sys
from src.services.scheduler_service import main as scheduler_main

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(scheduler_main())