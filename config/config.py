# config/config.py - добавьте настройки БД
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

class Settings:
    def __init__(self):
        # Keycloak настройки
        self.KEYCLOAK_BASE_URL = os.getenv("KEYCLOAK_BASE_URL")
        self.KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM")
        self.CLIENT_ID = os.getenv("CLIENT_ID")
        self.CLIENT_SECRET = os.getenv("CLIENT_SECRET")

        # API настройки
        self.API_BASE_URL = os.getenv("API_BASE_URL")
        self.CERT_PATH = os.getenv("CERT_PATH")

        # Настройки БД
        self.DB_HOST = os.getenv("DB_HOST", "db23")
        self.DB_NAME = os.getenv("DB_NAME", "витринаданных")
        self.DB_DRIVER = os.getenv("DB_DRIVER", "SQL Server")
        self.DB_TRUSTED_CONNECTION = os.getenv("DB_TRUSTED_CONNECTION", "yes")

        # Пути
        self.TOKEN_FILE = "api_merch_token.json"
        self.NETWORK_FOLDER = r"\\vra.local\root\Public\ОИС\ПАО_Магнит"

        # Настройки шедулера
        self.CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "120"))
        self.DAILY_CHECK_TIME = os.getenv("DAILY_CHECK_TIME", "08:00")

settings = Settings()