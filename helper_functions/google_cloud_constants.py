import os
from enum import Enum

from dotenv import load_dotenv

load_dotenv()


class GoogleCloud(Enum):
    PROJECT_ID = os.environ['DAILY_SALES_BUCKET']
    DAILY_SALES_BUCKET = os.environ['DAILY_SALES_BUCKET']
    FILE_PATH = os.environ['FILE_PATH']
