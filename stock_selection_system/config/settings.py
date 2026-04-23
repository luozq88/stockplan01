import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

TUSHARE_TOKEN = '2f4f7b0dca606122c89b03503ebb70c4b26652328f446848a69e71e2'

DATABASE_PATH = BASE_DIR / 'data' / 'stock_data.db'
DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

TRADING_DAYS_COUNT = 150

DATA_UPDATE_TIME = '16:00'
TECHNICAL_CALCULATION_TIME = '16:30'
STOCK_SELECTION_TIME = '11:30'

VOLUME_RATIO_THRESHOLD = 1.5
PRICE_CHANGE_MIN = 1.8
PRICE_CHANGE_MAX = 8
MA89_PERIOD = 89

REQUEST_INTERVAL = 0.5
MAX_RETRY_TIMES = 3
BATCH_INSERT_SIZE = 1000

LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = BASE_DIR / 'logs' / 'stock_selection.log'
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

TASK_SCHEDULE = {
    'data_update': {
        'time': DATA_UPDATE_TIME,
        'enabled': True,
        'description': '每日收盘后更新股票数据'
    },
    'technical_calculation': {
        'time': TECHNICAL_CALCULATION_TIME,
        'enabled': True,
        'description': '计算技术指标'
    },
    'stock_selection': {
        'time': STOCK_SELECTION_TIME,
        'enabled': True,
        'description': '午盘选股'
    }
}
