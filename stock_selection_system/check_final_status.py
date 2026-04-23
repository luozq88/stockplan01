import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.database import DatabaseManager

def check_final_status():
    db = DatabaseManager()
    
    print("=" * 60)
    print("数据库最终状态")
    print("=" * 60)
    print(f"股票信息: {db.get_data_count('stock_info'):,} 只")
    print(f"日线数据: {db.get_data_count('daily_data'):,} 条")
    print(f"技术指标: {db.get_data_count('technical_indicators'):,} 条")
    print(f"选股结果: {db.get_data_count('stock_selection'):,} 条")
    print("=" * 60)
    
    summary = db.get_update_summary()
    print("\n更新摘要:")
    for key, value in summary.items():
        print(f"  {key}: {value}")

if __name__ == '__main__':
    check_final_status()
