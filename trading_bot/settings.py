import logging
import os
from datetime import datetime
from pathlib import Path

import pytz

BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / 'logs'
CONFIG_DIR = BASE_DIR / 'config'

TZ = pytz.timezone('Asia/Kolkata')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(message)s')

if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)
file_handler = logging.FileHandler(LOGS_DIR / f'{datetime.now(tz=TZ).date()}_trades.log')

file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def customTime(*args):
    utc_dt = pytz.utc.localize(datetime.utcnow())
    converted = utc_dt.astimezone(TZ)
    return converted.timetuple()


# Set logs timezone
logging.Formatter.converter = customTime
