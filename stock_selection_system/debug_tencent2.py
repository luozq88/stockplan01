import requests
import re

url = "http://qt.gtimg.cn/q=sz000001,sh600519"
response = requests.get(url, timeout=10)
response.encoding = 'gbk'

data_str = response.text

# 测试新的正则表达式
matches = re.findall(r'v_([^=]+)="([^"]*)"', data_str)
print(f"找到 {len(matches)} 个匹配\n")

for code, values in matches:
    print(f"代码: {code}")
    parts = values.split('~')
    print(f"  股票名: {parts[1] if len(parts) > 1 else 'N/A'}")
    print(f"  当前价: {parts[3] if len(parts) > 3 else 'N/A'}")
    print(f"  涨跌幅: {parts[32] if len(parts) > 32 else 'N/A'}%")
    print(f"  开盘价: {parts[5] if len(parts) > 5 else 'N/A'}")
    print(f"  昨收: {parts[4] if len(parts) > 4 else 'N/A'}")
    print()
