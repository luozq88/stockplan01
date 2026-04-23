import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logger import setup_logger
from core.database import DatabaseManager
from core.data_fetcher import DataFetcher
from core.technical_analysis import TechnicalAnalysis
from core.stock_selector import StockSelector
from utils.scheduler import TaskScheduler
from clients.tushare_client import TushareClient
from config.settings import TASK_SCHEDULE
import argparse
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description='股票数据获取与选股系统')
    parser.add_argument('--mode', choices=['init', 'update', 'select', 'schedule', 'test'], 
                       default='update', help='运行模式')
    parser.add_argument('--date', type=str, help='指定日期 (YYYYMMDD)')
    parser.add_argument('--stock', type=str, help='指定股票代码')
    
    args = parser.parse_args()
    
    logger = setup_logger(__name__)
    logger.info("=" * 60)
    logger.info("股票数据获取与选股系统启动")
    logger.info(f"运行模式: {args.mode}")
    logger.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    try:
        if args.mode == 'init':
            run_init_mode(logger)
        elif args.mode == 'update':
            run_update_mode(logger, args.stock)
        elif args.mode == 'select':
            run_select_mode(logger, args.date)
        elif args.mode == 'schedule':
            run_schedule_mode(logger)
        elif args.mode == 'test':
            run_test_mode(logger)
            
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行错误: {e}", exc_info=True)
    finally:
        logger.info("=" * 60)
        logger.info("程序运行结束")
        logger.info("=" * 60)


def run_init_mode(logger):
    logger.info("初始化模式：更新股票列表和所有数据")
    
    db_manager = DatabaseManager()
    tushare_client = TushareClient()
    data_fetcher = DataFetcher(db_manager, tushare_client)
    
    logger.info("步骤1: 测试Tushare连接...")
    if not tushare_client.test_connection():
        logger.error("Tushare连接测试失败")
        return
    
    logger.info("步骤2: 更新股票列表...")
    stock_count = data_fetcher.update_stock_list()
    logger.info(f"更新了 {stock_count} 只股票")
    
    logger.info("步骤3: 更新日线数据...")
    stats = data_fetcher.update_daily_data()
    logger.info(f"数据更新统计: {stats}")
    
    logger.info("步骤4: 计算技术指标...")
    tech_analysis = TechnicalAnalysis(db_manager)
    stocks = db_manager.get_stock_list()
    ts_codes = [stock['ts_code'] for stock in stocks]
    tech_stats = tech_analysis.batch_update_indicators(ts_codes)
    logger.info(f"技术指标计算统计: {tech_stats}")
    
    logger.info("初始化完成")


def run_update_mode(logger, stock_code=None):
    logger.info("更新模式：增量更新数据")
    
    db_manager = DatabaseManager()
    tushare_client = TushareClient()
    data_fetcher = DataFetcher(db_manager, tushare_client)
    
    logger.info("步骤1: 更新股票列表...")
    stock_count = data_fetcher.update_stock_list()
    logger.info(f"更新了 {stock_count} 只股票")
    
    logger.info("步骤2: 更新日线数据...")
    if stock_code:
        success = data_fetcher.update_single_stock(stock_code)
        logger.info(f"更新股票 {stock_code}: {'成功' if success else '失败'}")
    else:
        stats = data_fetcher.update_daily_data()
        logger.info(f"数据更新统计: {stats}")
    
    logger.info("步骤3: 计算技术指标...")
    tech_analysis = TechnicalAnalysis(db_manager)
    
    if stock_code:
        success = tech_analysis.update_technical_indicators(stock_code)
        logger.info(f"技术指标计算 {stock_code}: {'成功' if success else '失败'}")
    else:
        stocks = db_manager.get_stock_list()
        ts_codes = [stock['ts_code'] for stock in stocks]
        tech_stats = tech_analysis.batch_update_indicators(ts_codes)
        logger.info(f"技术指标计算统计: {tech_stats}")
    
    logger.info("更新完成")


def run_select_mode(logger, date=None):
    logger.info("选股模式：执行选股策略")
    
    db_manager = DatabaseManager()
    stock_selector = StockSelector(db_manager)
    
    if date is None:
        date = datetime.now().strftime('%Y%m%d')
    
    logger.info(f"选股日期: {date}")
    logger.info("执行午盘选股策略...")
    
    selected_stocks = stock_selector.select_stocks_noon(date)
    
    logger.info(f"选中 {len(selected_stocks)} 只股票")
    
    if selected_stocks:
        logger.info("\n选股结果:")
        logger.info("-" * 80)
        for i, stock in enumerate(selected_stocks, 1):
            logger.info(f"{i}. {stock['ts_code']} - 涨幅: {stock['pct_chg']:.2f}%, "
                       f"量比: {stock['volume_ratio']:.2f}, "
                       f"89日线: {stock['ma89_value']:.2f}")
            logger.info(f"   选股理由: {stock['selection_reason']}")
        logger.info("-" * 80)
        
        export_file = f"selection_results_{date}.csv"
        if stock_selector.export_selection_results(date, export_file):
            logger.info(f"选股结果已导出到: {export_file}")
    else:
        logger.info("今日没有符合条件的股票")


def run_schedule_mode(logger):
    logger.info("定时任务模式：启动定时任务调度器")
    
    db_manager = DatabaseManager()
    tushare_client = TushareClient()
    data_fetcher = DataFetcher(db_manager, tushare_client)
    tech_analysis = TechnicalAnalysis(db_manager)
    stock_selector = StockSelector(db_manager)
    
    def task_data_update():
        logger.info("执行定时任务: 更新数据")
        data_fetcher.update_stock_list()
        stats = data_fetcher.update_daily_data()
        logger.info(f"数据更新完成: {stats}")
    
    def task_technical_calculation():
        logger.info("执行定时任务: 计算技术指标")
        stocks = db_manager.get_stock_list()
        ts_codes = [stock['ts_code'] for stock in stocks]
        tech_stats = tech_analysis.batch_update_indicators(ts_codes)
        logger.info(f"技术指标计算完成: {tech_stats}")
    
    def task_stock_selection():
        logger.info("执行定时任务: 选股")
        selected_stocks = stock_selector.select_stocks_noon()
        logger.info(f"选股完成，选中 {len(selected_stocks)} 只股票")
    
    scheduler = TaskScheduler()
    
    task_functions = {
        'data_update': task_data_update,
        'technical_calculation': task_technical_calculation,
        'stock_selection': task_stock_selection
    }
    
    scheduler.setup_from_config(task_functions)
    
    logger.info("定时任务列表:")
    for task_name, task_info in scheduler.list_tasks().items():
        logger.info(f"  - {task_name}: {task_info['time']}")
    
    logger.info("启动定时任务调度器...")
    scheduler.start()


def run_test_mode(logger):
    logger.info("测试模式：测试系统功能")
    
    db_manager = DatabaseManager()
    tushare_client = TushareClient()
    
    logger.info("测试1: 数据库连接...")
    try:
        summary = db_manager.get_update_summary()
        logger.info(f"数据库统计: {summary}")
    except Exception as e:
        logger.error(f"数据库测试失败: {e}")
    
    logger.info("测试2: Tushare连接...")
    if tushare_client.test_connection():
        logger.info("Tushare连接测试成功")
    else:
        logger.error("Tushare连接测试失败")
    
    logger.info("测试3: 获取股票列表...")
    df = tushare_client.get_stock_list()
    if not df.empty:
        logger.info(f"获取到 {len(df)} 只股票")
        logger.info(f"示例股票:\n{df.head()}")
    else:
        logger.warning("未获取到股票数据")
    
    logger.info("测试完成")


if __name__ == '__main__':
    main()
