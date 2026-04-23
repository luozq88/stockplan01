from datetime import datetime, timedelta
from typing import List, Optional
import pandas as pd


def get_trade_dates(start_date: str, end_date: str, trade_calendar: List[str]) -> List[str]:
    return [date for date in trade_calendar if start_date <= date <= end_date]


def is_trading_day(date: str = None, trade_calendar: List[str] = None) -> bool:
    if date is None:
        date = datetime.now().strftime('%Y%m%d')
    
    if trade_calendar is None:
        weekday = datetime.strptime(date, '%Y%m%d').weekday()
        return weekday < 5
    
    return date in trade_calendar


def get_next_trading_day(date: str, trade_calendar: List[str]) -> Optional[str]:
    try:
        idx = trade_calendar.index(date)
        if idx < len(trade_calendar) - 1:
            return trade_calendar[idx + 1]
        return None
    except ValueError:
        return None


def get_previous_trading_day(date: str, trade_calendar: List[str]) -> Optional[str]:
    try:
        idx = trade_calendar.index(date)
        if idx > 0:
            return trade_calendar[idx - 1]
        return None
    except ValueError:
        return None


def format_percentage(value: float) -> str:
    return f"{value:.2f}%"


def format_number(value: float, decimal: int = 2) -> str:
    return f"{value:,.{decimal}f}"


def calculate_profit_rate(buy_price: float, sell_price: float) -> float:
    if buy_price == 0:
        return 0.0
    return (sell_price - buy_price) / buy_price * 100


def validate_stock_code(ts_code: str) -> bool:
    if not ts_code:
        return False
    
    parts = ts_code.split('.')
    if len(parts) != 2:
        return False
    
    symbol, market = parts
    
    if not symbol.isdigit() or len(symbol) != 6:
        return False
    
    if market not in ['SH', 'SZ']:
        return False
    
    return True


def get_market_from_code(ts_code: str) -> Optional[str]:
    if not validate_stock_code(ts_code):
        return None
    
    return ts_code.split('.')[1]


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
    
    return df


def export_to_csv(data: List[dict], filename: str, encoding: str = 'utf-8-sig') -> bool:
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding=encoding)
        return True
    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        return False


def get_current_time() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_current_date() -> str:
    return datetime.now().strftime('%Y%m%d')


def is_market_open() -> bool:
    now = datetime.now()
    current_time = now.time()
    
    morning_open = datetime.strptime('09:30', '%H:%M').time()
    morning_close = datetime.strptime('11:30', '%H:%M').time()
    afternoon_open = datetime.strptime('13:00', '%H:%M').time()
    afternoon_close = datetime.strptime('15:00', '%H:%M').time()
    
    if morning_open <= current_time <= morning_close:
        return True
    if afternoon_open <= current_time <= afternoon_close:
        return True
    
    return False
