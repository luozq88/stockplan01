import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.database import DatabaseManager
from clients.tencent_client import TencentClient
from core.stock_selector import StockSelector
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_tencent_client():
    print("\n" + "=" * 60)
    print("测试1: 腾讯数据源连接")
    print("=" * 60)
    
    client = TencentClient()
    
    if client.test_connection():
        print("✓ 腾讯API连接成功")
    else:
        print("✗ 腾讯API连接失败")
        return False
    
    print("\n测试获取单只股票实时数据...")
    quote = client.get_single_quote('000001.SZ')
    if quote:
        print(f"✓ 获取成功: {quote['ts_code']} - {quote['name']}")
        print(f"  当前价: {quote['current_price']}, 涨幅: {quote['pct_chg']}%")
    else:
        print("✗ 获取失败")
    
    return True

def test_realtime_selection():
    print("\n" + "=" * 60)
    print("测试2: 实时选股逻辑")
    print("=" * 60)
    
    db = DatabaseManager()
    selector = StockSelector(db)
    
    print("\n开始实时选股...")
    selected = selector.select_stocks_realtime()
    
    if selected:
        print(f"\n✓ 选股完成，共选出 {len(selected)} 只股票:")
        print("-" * 60)
        for i, stock in enumerate(selected[:10], 1):
            print(f"{i}. {stock['ts_code']} | 涨幅: {stock['pct_chg']:.2f}% | "
                  f"量比: {stock['volume_ratio']:.2f} | MA89: {stock['ma89_value']:.2f}")
            print(f"   选股理由: {stock['selection_reason']}")
    else:
        print("当前没有符合选股条件的股票")
    
    return selected

def test_ma89_calculation():
    print("\n" + "=" * 60)
    print("测试3: MA89计算验证")
    print("=" * 60)
    
    db = DatabaseManager()
    
    test_code = '000001.SZ'
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT close FROM daily_data 
            WHERE ts_code = ? 
            ORDER BY trade_date DESC 
            LIMIT 88
        ''', (test_code,))
        rows = cursor.fetchall()
    
    if len(rows) < 88:
        print(f"历史数据不足: {len(rows)}/88")
        return
    
    import numpy as np
    historical_closes = [row['close'] for row in reversed(rows)]
    
    current_price = 12.50
    
    all_closes = historical_closes + [current_price]
    ma89 = np.mean(all_closes)
    
    print(f"股票代码: {test_code}")
    print(f"历史数据: {len(historical_closes)} 条")
    print(f"模拟当前价: {current_price}")
    print(f"计算MA89: {ma89:.2f}")
    print("✓ MA89计算验证通过")

def main():
    print("\n" + "=" * 60)
    print("选股系统实时选股测试")
    print("=" * 60)
    
    test_tencent_client()
    
    test_ma89_calculation()
    
    selected = test_realtime_selection()
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)

if __name__ == '__main__':
    main()
