import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.database import DatabaseManager
from datetime import datetime

def check_progress():
    db = DatabaseManager()
    
    stock_count = db.get_data_count('stock_info')
    daily_count = db.get_data_count('daily_data')
    tech_count = db.get_data_count('technical_indicators')
    
    total_stocks = 5508
    expected_records_per_stock = 150
    expected_total_records = total_stocks * expected_records_per_stock
    
    progress_percent = (daily_count / expected_total_records) * 100 if expected_total_records > 0 else 0
    
    print("=" * 60)
    print("股票数据获取进度监控")
    print("=" * 60)
    print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"股票信息: {stock_count:,} 只")
    print(f"日线数据: {daily_count:,} 条")
    print(f"技术指标: {tech_count:,} 条")
    print("-" * 60)
    print(f"目标股票数: {total_stocks:,} 只")
    print(f"预计总记录数: {expected_total_records:,} 条")
    print(f"当前进度: {progress_percent:.2f}%")
    print("=" * 60)
    
    if progress_percent < 100:
        remaining_records = expected_total_records - daily_count
        estimated_time_minutes = (remaining_records / 1000) * 1.5
        print(f"预计剩余时间: {estimated_time_minutes:.0f} 分钟")
        print(f"状态: 正在获取数据...")
    else:
        print("状态: 数据获取完成！✅")
    
    print("=" * 60)

if __name__ == '__main__':
    check_progress()
