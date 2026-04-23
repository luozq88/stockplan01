from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StockInfo:
    ts_code: str
    symbol: str
    name: str
    area: Optional[str] = None
    industry: Optional[str] = None
    list_date: Optional[str] = None
    updated_at: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'ts_code': self.ts_code,
            'symbol': self.symbol,
            'name': self.name,
            'area': self.area,
            'industry': self.industry,
            'list_date': self.list_date,
            'updated_at': self.updated_at or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'StockInfo':
        return cls(
            ts_code=data['ts_code'],
            symbol=data['symbol'],
            name=data['name'],
            area=data.get('area'),
            industry=data.get('industry'),
            list_date=data.get('list_date'),
            updated_at=data.get('updated_at')
        )


@dataclass
class DailyData:
    ts_code: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    pre_close: float
    change: float
    pct_chg: float
    vol: float
    amount: float
    ma89: Optional[float] = None
    volume_ratio: Optional[float] = None
    is_breakthrough: Optional[int] = 0
    
    def to_dict(self) -> dict:
        return {
            'ts_code': self.ts_code,
            'trade_date': self.trade_date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'pre_close': self.pre_close,
            'change': self.change,
            'pct_chg': self.pct_chg,
            'vol': self.vol,
            'amount': self.amount,
            'ma89': self.ma89,
            'volume_ratio': self.volume_ratio,
            'is_breakthrough': self.is_breakthrough
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DailyData':
        return cls(
            ts_code=data['ts_code'],
            trade_date=data['trade_date'],
            open=data['open'],
            high=data['high'],
            low=data['low'],
            close=data['close'],
            pre_close=data['pre_close'],
            change=data['change'],
            pct_chg=data['pct_chg'],
            vol=data['vol'],
            amount=data['amount'],
            ma89=data.get('ma89'),
            volume_ratio=data.get('volume_ratio'),
            is_breakthrough=data.get('is_breakthrough', 0)
        )


@dataclass
class TechnicalIndicators:
    ts_code: str
    trade_date: str
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma30: Optional[float] = None
    ma60: Optional[float] = None
    ma89: Optional[float] = None
    ma120: Optional[float] = None
    ma250: Optional[float] = None
    volume_avg_5: Optional[float] = None
    volume_avg_10: Optional[float] = None
    volume_ratio: Optional[float] = None
    rsi_6: Optional[float] = None
    rsi_12: Optional[float] = None
    rsi_24: Optional[float] = None
    updated_at: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'ts_code': self.ts_code,
            'trade_date': self.trade_date,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma30': self.ma30,
            'ma60': self.ma60,
            'ma89': self.ma89,
            'ma120': self.ma120,
            'ma250': self.ma250,
            'volume_avg_5': self.volume_avg_5,
            'volume_avg_10': self.volume_avg_10,
            'volume_ratio': self.volume_ratio,
            'rsi_6': self.rsi_6,
            'rsi_12': self.rsi_12,
            'rsi_24': self.rsi_24,
            'updated_at': self.updated_at or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TechnicalIndicators':
        return cls(
            ts_code=data['ts_code'],
            trade_date=data['trade_date'],
            ma5=data.get('ma5'),
            ma10=data.get('ma10'),
            ma20=data.get('ma20'),
            ma30=data.get('ma30'),
            ma60=data.get('ma60'),
            ma89=data.get('ma89'),
            ma120=data.get('ma120'),
            ma250=data.get('ma250'),
            volume_avg_5=data.get('volume_avg_5'),
            volume_avg_10=data.get('volume_avg_10'),
            volume_ratio=data.get('volume_ratio'),
            rsi_6=data.get('rsi_6'),
            rsi_12=data.get('rsi_12'),
            rsi_24=data.get('rsi_24'),
            updated_at=data.get('updated_at')
        )
