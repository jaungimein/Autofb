
import os
import logging
from dotenv import load_dotenv
from os import environ
from requests import get as rget

# Logger setup
LOG_FILE = "bot_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sharing_bot")

CONFIG_FILE_URL = environ.get('CONFIG_FILE_URL')
try:
    if len(CONFIG_FILE_URL) == 0:
        raise TypeError
    try:
        res = rget(CONFIG_FILE_URL)
        if res.status_code == 200:
            with open('config.env', 'wb+') as f:
                f.write(res.content)
        else:
            logger.error(f"Failed to download config.env {res.status_code}")
    except Exception as e:
        logger.info(f"CONFIG_FILE_URL: {e}")
except:
    pass

load_dotenv('config.env', override=True)

#TELEGRAM API
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID = int(os.getenv('OWNER_ID'))
BOT_USERNAME = os.getenv('BOT_USERNAME')
UPDATE_CHANNEL_ID = int(os.getenv('UPDATE_CHANNEL_ID'))
GROUP_ID = int(os.getenv('GROUP_ID', 0))
UPDATE_CHANNEL_LINK = os.getenv('UPDATE_CHANNEL_LINK', 'https://t.me/')
SUPPORT = os.getenv('SUPPORT', 'https://t.me/')

TMDB_CHANNEL_ID = os.getenv('TMDB_CHANNEL_ID', '').split(',')
LOG_CHANNEL_ID = int(os.getenv('LOG_CHANNEL_ID'))

MY_DOMAIN = os.getenv('MY_DOMAIN')

TOKEN_VALIDITY_SECONDS = 24 * 60 * 60  # 24 hours

MONGO_URI = os.getenv("MONGO_URI")

TMDB_API_KEY = os.getenv('TMDB_API_KEY')
IMGBB_API_KEY = os.getenv('IMGBB_API_KEY')

#SHORTERNER API
URLSHORTX_API_TOKEN = os.getenv('URLSHORTX_API_TOKEN')
SHORTERNER_URL = os.getenv('SHORTERNER_URL')
