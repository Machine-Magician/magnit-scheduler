# scripts/test_imports.py
import sys
import os

# Добавляем корень проекта в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("=" * 50)
print("Тест импортов проекта")
print("=" * 50)

# Тестируем все импорты
try:
    from config.config import settings
    print("✅ config.config.settings")
except ImportError as e:
    print(f"❌ config.config: {e}")

try:
    from src.api.contractor_client import Token, API
    print("✅ src.api.contractor_client")
except ImportError as e:
    print(f"❌ contractor_client: {e}")

try:
    from src.services.exchange_service import main
    print("✅ src.services.exchange_service")
except ImportError as e:
    print(f"❌ exchange_service: {e}")

try:
    from src.services.utils import save_to_network_folder
    print("✅ src.services.utils")
except ImportError as e:
    print(f"❌ utils: {e}")

print("=" * 50)
print("Тест завершен")
print("=" * 50)