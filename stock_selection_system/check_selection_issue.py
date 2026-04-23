import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.database import DatabaseManager
from clients.tencent_client import TencentClient
import numpy as np

db = DatabaseManager()
client = TencentClient()

# 测试一只股票
ts_code = '301122.SZ'

print(f"检查股票 {ts_code} 的数据...")
print("=" * 60)

# 1. 检查数据库中的历史数据
with db.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute('''
        SELECT trade_date, close, vol 
        FROM daily_data 
        WHERE ts_code = ? 
        ORDER BY trade_date DESC 
        LIMIT 10
    ''', (ts_code,))
    rows = cursor.fetchall()
    
    print(f"\n最近10天的数据:")
    for row in rows:
        print(f"  {row['trade_date']}: 收盘价={row['close']}, 成交量={row['vol']}")

# 2. 获取实时行情
print(f"\n实时行情:")
quote = client.get_single_quote(ts_code)
if quote:
    print(f"  当前价: {quote['current_price']}")
    print(f"  涨跌幅: {quote['pct_chg']}%")
    print(f"  最低价: {quote['low']}")
    print(f"  成交量: {quote['vol']}")

# 3. 检查历史数据数量
with db.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) as count
        FROM daily_data 
        WHERE ts_code = ?
    ''', (ts_code,))
    result = cursor.fetchone()
    print(f"\n数据库中总共有 {result['count']} 条历史数据")

# 4. 计算MA89
with db.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute('''
        SELECT close
        FROM daily_data 
        WHERE ts_code = ? 
        ORDER BY trade_date DESC 
        LIMIT 88
    ''', (ts_code,))
    rows = cursor.fetchall()
    
    historical_closes = [row['close'] for row in reversed(rows)]
    print(f"\n获取到 {len(historical_closes)} 个历史收盘价")
    
    if quote and len(historical_closes) > 0:
        all_closes = historical_closes + [quote['current_price']]
        ma89 = np.mean(all_closes)
        print(f"计算出的MA89: {ma89:.2f}")
        print(f"当前价格: {quote['current_price']}")
        print(f"最低价: {quote['low']}")
        print(f"是否突破: 最低价{quote['low']} < MA89{ma89:.2f} < 当前价{quote['current_price']}")
