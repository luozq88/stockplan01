from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StockSelection:
    ts_code: str
    trade_date: str
    selection_time: str
    open_price: float
    current_price: float
    ma89_value: float
    volume_ratio: float
    pct_chg: float
    breakthrough_type: int
    selection_reason: str
    created_at: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'ts_code': self.ts_code,
            'trade_date': self.trade_date,
            'selection_time': self.selection_time,
            'open_price': self.open_price,
            'current_price': self.current_price,
            'ma89_value': self.ma89_value,
            'volume_ratio': self.volume_ratio,
            'pct_chg': self.pct_chg,
            'breakthrough_type': self.breakthrough_type,
            'selection_reason': self.selection_reason,
            'created_at': self.created_at or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'StockSelection':
        return cls(
            ts_code=data['ts_code'],
            trade_date=data['trade_date'],
            selection_time=data['selection_time'],
            open_price=data['open_price'],
            current_price=data['current_price'],
            ma89_value=data['ma89_value'],
            volume_ratio=data['volume_ratio'],
            pct_chg=data['pct_chg'],
            breakthrough_type=data['breakthrough_type'],
            selection_reason=data['selection_reason'],
            created_at=data.get('created_at')
        )


@dataclass
class SelectionResult:
    ts_code: str
    name: str
    trade_date: str
    selection_time: str
    current_price: float
    ma89_value: float
    volume_ratio: float
    pct_chg: float
    selection_reason: str
    industry: Optional[str] = None
    
    def to_display_string(self) -> str:
        return (
            f"股票代码: {self.ts_code}\n"
            f"股票名称: {self.name}\n"
            f"选股日期: {self.trade_date}\n"
            f"选股时间: {self.selection_time}\n"
            f"当前价格: {self.current_price:.2f}\n"
            f"89日线: {self.ma89_value:.2f}\n"
            f"量比: {self.volume_ratio:.2f}\n"
            f"涨幅: {self.pct_chg:.2f}%\n"
            f"选股理由: {self.selection_reason}\n"
            f"所属行业: {self.industry or '未知'}"
        )
