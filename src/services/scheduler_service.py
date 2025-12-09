# src/services/scheduler_service.py - с интервалом 2 часа
import asyncio
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from config.config import settings
from src.api.contractor_client import Token
from src.services.exchange_service import main as main_process
from src.services.utils import process_results
from src.services.schedule_service import create_schedules_only, upload_schedules_to_api


class DatabaseChecker:
    """Класс для проверки условий в БД"""

    def __init__(self):
        self.connection_string = (
            f"mssql+pyodbc://{settings.DB_HOST}/{settings.DB_NAME}"
            f"?trusted_connection={settings.DB_TRUSTED_CONNECTION}"
            f"&driver={settings.DB_DRIVER.replace(' ', '+')}"
        )
        self.engine = create_engine(self.connection_string)

    def check_schedule_condition(self) -> tuple[bool, list]:
        """Проверяет, нужно ли запускать создание графиков"""
        try:
            today = datetime.now().strftime("%Y-%m-%d")

            # Уточненный запрос - проверяем наличие данных на сегодня
            query = """
            SELECT DISTINCT магазин 
            FROM [витринаданных].[dbo].[План_факт_кз]
            WHERE дата = :today AND магазин IS NOT NULL
            """

            with self.engine.connect() as conn:
                df = pd.read_sql_query(
                    sql=text(query),
                    con=conn,
                    params={"today": today}
                )

            if len(df) > 0:
                shop_codes = df['магазин'].astype(str).tolist()
                print(f"✅ В БД найдены магазины на сегодня ({today}): {len(shop_codes)} шт.")
                print(f"   Примеры: {shop_codes[:5] if shop_codes else 'нет данных'}")
                return True, shop_codes
            else:
                print(f"⏸️ В БД нет данных на сегодня ({today})")
                return False, []

        except Exception as e:
            print(f"❌ Ошибка проверки БД: {e}")
            return False, []


class SchedulerApp:
    """Приложение-шедулер с проверкой БД каждые 2 часа"""

    def __init__(self, check_interval_minutes: int = None):
        self.scheduler = AsyncIOScheduler()
        self.token_obj = None
        self.db_checker = DatabaseChecker()

        # Используем настройку из config или 120 минут по умолчанию
        self.check_interval = check_interval_minutes or settings.CHECK_INTERVAL_MINUTES

    async def initialize(self):
        """Инициализация токена"""
        print("\n🔑 Создаем объект токена...")
        self.token_obj = Token(
            filename=settings.TOKEN_FILE,
            client_id=settings.CLIENT_ID,
            client_secret=settings.CLIENT_SECRET,
            base_url=settings.KEYCLOAK_BASE_URL,
            realm=settings.KEYCLOAK_REALM
        )

    async def run_full_process(self, shop_codes: list = None):
        """Полный процесс обмена"""
        print(f"\n🚀 Запуск полного процесса: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            # 1. Основной процесс (получение сигналов)
            print("\n📡 1. Получаем данные от контрагента...")
            results = await main_process(
                base_url=settings.API_BASE_URL,
                token_obj=self.token_obj
            )

            if results is not None:
                process_results(results)

            # 2. Создаем графики
            print("\n📅 2. Создаем и отправляем графики...")
            schedules_result = await create_schedules_only(
                base_url=settings.API_BASE_URL,
                token_obj=self.token_obj
            )

            await upload_schedules_to_api(
                base_url=settings.API_BASE_URL,
                token_obj=self.token_obj,
                schedules_data=schedules_result
            )

            print(f"\n✅ Процесс завершен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            print(f"❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()

    def start_scheduler(self):
        """Запуск шедулера с проверкой каждые 2 часа"""

        # Основная проверка: каждые 2 часа
        self.scheduler.add_job(
            self._check_and_run,
            trigger=IntervalTrigger(minutes=self.check_interval),
            id='db_check_job',
            name='Проверка БД каждые 2 часа'
        )

        # Дополнительная проверка утром (если нужно)
        if hasattr(settings, 'DAILY_CHECK_TIME'):
            hour, minute = map(int, settings.DAILY_CHECK_TIME.split(':'))
            self.scheduler.add_job(
                self._check_and_run,
                trigger=CronTrigger(hour=hour, minute=minute),
                id='daily_check_job',
                name='Ежедневная проверка'
            )

        # Запуск при старте
        self.scheduler.add_job(
            self._check_and_run,
            trigger='date',
            run_date=datetime.now(),
            id='initial_check_job',
            name='Начальная проверка'
        )

        self.scheduler.start()
        print(f"⏰ Шедулер запущен. Проверка БД каждые {self.check_interval} минут ({self.check_interval/60:.1f} часов)...")
        print(f"   Следующая проверка в: {(datetime.now() + timedelta(minutes=self.check_interval)).strftime('%H:%M')}")

    async def _check_and_run(self):
        """Проверка условия и запуск процесса"""
        print(f"\n{'='*60}")
        print(f"🔍 Проверка БД: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

        should_run, shop_codes = self.db_checker.check_schedule_condition()

        if should_run:
            print("✅ Условие выполнено, запускаем процесс...")
            await self.run_full_process(shop_codes)
        else:
            next_check = datetime.now() + timedelta(minutes=self.check_interval)
            print(f"⏸️ Условие не выполнено")
            print(f"   Следующая проверка: {next_check.strftime('%Y-%m-%d %H:%M:%S')}")

    async def run_once(self):
        """Запуск однократной проверки и выполнения"""
        await self.initialize()
        await self._check_and_run()

    def shutdown(self):
        """Остановка шедулера"""
        self.scheduler.shutdown()
        print("🛑 Шедулер остановлен")


async def main():
    """Точка входа для шедулера"""
    print("="*60)
    print("🚀 Шедулер обмена данными с контрагентом")
    print("="*60)
    print(f"Настройки:")
    print(f"  • Проверка БД: каждые {settings.CHECK_INTERVAL_MINUTES} мин ({settings.CHECK_INTERVAL_MINUTES/60:.1f} ч)")
    print(f"  • База данных: {settings.DB_HOST}/{settings.DB_NAME}")
    print(f"  • API: {settings.API_BASE_URL}")
    print("="*60)

    app = SchedulerApp()

    try:
        await app.initialize()
        app.start_scheduler()

        # Бесконечный цикл с отображением статуса
        while True:
            await asyncio.sleep(300)  # Спим 5 минут, но выводим статус

            # Можно добавить периодический вывод статуса
            if datetime.now().minute % 30 == 0:  # Каждые 30 минут
                print(f"\n📊 Статус: Шедулер работает ({datetime.now().strftime('%H:%M')})")

    except (KeyboardInterrupt, SystemExit):
        print("\n👋 Остановка по команде пользователя...")
        app.shutdown()
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        app.shutdown()


async def manual_run():
    """Ручной запуск (для тестов)"""
    app = SchedulerApp()
    await app.run_once()


if __name__ == "__main__":
    asyncio.run(main())