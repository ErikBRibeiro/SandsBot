import os
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv('BYBIT_API_KEY')
API_SECRET = os.getenv('BYBIT_API_SECRET')

if not API_KEY or not API_SECRET:
    raise ValueError("API Key ou API Secret não encontrada. Verifique o arquivo .env.")
