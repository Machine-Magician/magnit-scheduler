# src/services/schedule_service.py
import asyncio
import httpx
from datetime import datetime, timezone, date, timedelta
from typing import List, Dict, Any, Optional

from src.api.contractor_client import Token, API


async def create_schedules_only(base_url: str, token_obj: Token) -> List[Dict[str, Any]]:
    """Создание графиков - ИСПРАВЛЕННАЯ версия под ваш API"""

    async with httpx.AsyncClient(base_url=base_url, verify=False, timeout=30.0) as client:
        api = API(client=client)
        token = await token_obj.get_token()

        # Создаем графики на СЛЕДУЮЩИЙ день
        schedule_date = datetime.now(tz=timezone.utc).date() + timedelta(days=1)

        print(f"📅 Создаем графики на дату: {schedule_date}")

        # Список всех магазинов с именами
        shops_data = [
            ("111111", "Тестовый магазин"),
            ("993610", "Борисоглебск 1 Матросовская (а)"),
            ("993613", "Борисоглебск 2 Матросовская (а)"),
            ("993611", "Воронеж 3 Ростовская (а)"),
            ("993617", "Воронеж 4 (а)"),
            ("993614", "Воронеж 5 (а)"),
            ("994802", "Елец 1 Радиотехническая"),
            ("994601", "Курск 2 Кулакова (а)"),
            ("993609", "Лиски 1 Титова (а)"),
            ("993602", "Нововоронеж 1 Первомайская (а)"),
            ("997110", "Новомосковск 2 Кукунина"),
            ("993606", "Россошь 1 Простеева (а)"),
            ("993608", "Россошь 2 Труда (а)"),
            ("993101", "Старый Оскол 1 Молодежный (а)"),
            ("993107", "Старый Оскол 2 Олимпийский (а)"),
            ("996801", "Тамбов 1 Советская"),
            ("997108", "Тула 2 Сойфера")
        ]

        print(f"   🏪 Создаем графики для {len(shops_data)} магазинов...")

        # ВАЖНО: ваш API метод post_merchandisers_schedules ожидает shops_data (список)
        result = await api.post_merchandisers_schedules(
            token=token,
            dt=schedule_date,
            shops_data=shops_data  # ← передаем ВЕСЬ список магазинов
        )

        # Формируем результат для каждого магазина
        schedule_results = []
        for shop_code, shop_name in shops_data:
            schedule_results.append({
                "shop_code": shop_code,
                "shop_name": shop_name,
                "result": result  # все магазины получают общий результат
            })

        print("   ✅ Все графики созданы")
        return schedule_results


async def upload_schedules_to_api(base_url: str, token_obj: Token, schedules_data: list):
    """Отправка графиков - ИСПРАВЛЕННАЯ версия под ваш API"""

    async with httpx.AsyncClient(base_url=base_url, verify=False, timeout=30.0) as client:
        api = API(client=client)
        token = await token_obj.get_token()

        # Дата для графика (следующий день)
        schedule_date = datetime.now(tz=timezone.utc).date() + timedelta(days=1)

        print(f"📤 Отправляем графики на дату {schedule_date}...")

        # Извлекаем shops_data из schedules_data
        shops_data = [(item["shop_code"], item.get("shop_name", f"Магазин {item['shop_code']}"))
                      for item in schedules_data]

        # ВАЖНО: отправляем ВСЕ магазины одним запросом
        result = await api.post_merchandisers_schedules(
            token=token,
            dt=schedule_date,
            shops_data=shops_data  # ← передаем список магазинов
        )

        if isinstance(result, dict) and "error" in result:
            print(f"❌ Ошибка отправки графиков: {result['error']}")
            return 0
        else:
            print(f"✅ Все графики отправлены успешно!")
            return len(shops_data)


async def delete_schedules(base_url: str, token_obj: Token, shops_list: list = None) -> Dict[str, Any]:
    """Функция для удаления графиков посещения - ОСТАВЛЯЕМ БЕЗ ИЗМЕНЕНИЙ"""
    print("🗑️ Запуск функции удаления графиков...")

    if shops_list is None:
        shops_list = [
            "111111",  # Тестовый магазин
            "993610", "993613", "993611", "993617", "993614",
            "994802", "994601", "993609", "993602", "997110",
            ("993606", "Россошь 1 Простеева (а)"),
            ("993608", "Россошь 2 Труда (а)"),
            ("993101", "Старый Оскол 1 Молодежный (а)"),
            ("993107", "Старый Оскол 2 Олимпийский (а)"),
            ("996801", "Тамбов 1 Советская"),
            ("997108", "Тула 2 Сойфера")
        ]

    async with httpx.AsyncClient(base_url=base_url, verify=False, timeout=30.0) as client:
        api = API(client=client)
        token = await token_obj.get_token()
        date_of_visit_shop = datetime.now(tz=timezone.utc).date()

        delete_results = []

        try:
            print(f"🗑️ Удаляем графики для {len(shops_list)} магазинов...")

            for i, shop_code in enumerate(shops_list, 1):
                print(f"   {i}/{len(shops_list)} Удаляем график для магазина {shop_code}...")

                try:
                    # delete_merchandisers_schedules ожидает shop_code (строку)
                    delete_result = await api.delete_merchandisers_schedules(
                        token=token,
                        dt=date_of_visit_shop,
                        shop_code=str(shop_code)  # преобразуем в строку
                    )
                    delete_results.append({"shop_code": shop_code, "result": delete_result})
                    print(f"      ✅ Удален")
                except Exception as e:
                    error_msg = f"Ошибка: {e}"
                    delete_results.append({"shop_code": shop_code, "result": error_msg})
                    print(f"      ❌ {error_msg}")

                await asyncio.sleep(0.3)

            print("✅ Удаление графиков завершено!")
            return {"delete_result": delete_results}

        except Exception as e:
            print(f"❌ Ошибка при удалении: {e}")
            return {"error": str(e)}