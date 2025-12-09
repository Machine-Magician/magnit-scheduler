# src/models/signal.py
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Dict, Any

@dataclass
class Signal:
    """Минимальная модель сигнала - только для импортов"""
    shop_code: str
    shop_name: str
    product_name: str
    problem_code: str
    problem_name: str
    signal_date: date

    # Дополнительные поля
    signal_id: Optional[str] = None
    id: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.now)
    status: str = "new"
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Schedule:
    """Модель графика посещений - для методов API"""
    date: date
    shop_code: str
    shop_name: str
    merch_name: str = ""
    merch_phone: str = ""
    duration: int = 30
    agency: str = ""
    gr20: str = ""

@dataclass
class TokenData:
    """Модель для хранения токена"""
    access_token: str
    refresh_token: str = ""
    expires_at: Optional[datetime] = None
    token_type: str = "Bearer"