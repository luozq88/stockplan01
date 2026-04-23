import requests
import re

url = "http://qt.gtimg.cn/q=sz000001,sh600519"
response = requests.get(url, timeout=10)
response.encoding = 'gbk'

print("原始响应文本:")
print(response.text[:500])
print("\n" + "="*60)

# 测试正则表达式匹配
data_str = response.text
matches = re.findall(r'v_"([^"]+)"="([^"]*)"', data_str)
print(f"找到 {len(matches)} 个匹配")

for code, values in matches[:5]:
    print(f"\n代码: {code}")
    print(f"值: {values[:100]}...")

    # 分割数据
    parts = values.split('~')
    if len(parts) >= 45:
        print(f"  股票名: {parts[1]}")
        print(f"  当前价: {parts[3]}")
        print(f"  涨跌幅: {parts[32]}%")
