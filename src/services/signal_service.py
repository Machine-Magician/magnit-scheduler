# src/services/signal_service.py
import asyncio
import httpx
from datetime import datetime, timezone
from typing import Dict, Any

from src.api.contractor_client import Token, API
from src.services.utils import prepare_signal_data
async def send_visit_signals(base_url: str, token_obj: Token):
    """Отправка сигналов о посещении - это новая функция - обратная связь после посещения"""

    async with httpx.AsyncClient(base_url=base_url, verify=False, timeout=30.0) as client:
        api = API(client=client)
        token = await token_obj.get_token()

        # Подготавливаем данные сигналов
        signals_to_send = [
            prepare_signal_data(
                shop_code="032146",
                shop_name="Ингул",
                product_code="1000236724",
                product_name="R.O.C.S Sensitive з/п Восстанов Отбел94г(Еврокосм):9/18",
                is_available=True,
                is_virtual_rest_risk=True,
                amount_virtual_rest_detected=23,
                is_product_missing=True,
                is_expiration_date_exceeded=True
            ),
            # Добавьте другие сигналы по необходимости
        ]

        print(f"🛜 Отправляем {len(signals_to_send)} сигналов о посещении...")

        # Отправляем сигналы
        result = await api.post_signals(
            token=token,
            signals_data=signals_to_send
        )

        return result