import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from clients.tencent_client import TencentClient

client = TencentClient()
ts_codes = ['000001.SZ', '600519.SH', '000002.SZ']

print(f"测试获取 {len(ts_codes)} 只股票实时行情...")
quotes = client.get_realtime_quotes(ts_codes)

print(f"获取到 {len(quotes)} 条行情")

if quotes:
    print("\n行情详情:")
    for q in quotes:
        print(f"  {q['ts_code']}: 价格={q['current_price']}, 涨幅={q['pct_chg']}%")
else:
    print("未能获取到任何行情数据")
