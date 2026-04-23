import logging
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import DatabaseManager
from clients.tushare_client import TushareClient
from config.settings import TRADING_DAYS_COUNT


class DataFetcher:
    def __init__(self, db_manager: DatabaseManager = None, tushare_client: TushareClient = None):
        self.db_manager = db_manager or DatabaseManager()
        self.tushare_client = tushare_client or TushareClient()
        self.logger = logging.getLogger(__name__)
    
    def update_stock_list(self) -> int:
        try:
            self.logger.info("Starting to update stock list...")
            
            df = self.tushare_client.get_stock_list()
            
            if df.empty:
                self.logger.warning("No stock data fetched")
                return 0
            
            stock_list = df.to_dict('records')
            
            inserted_count = self.db_manager.insert_stock_info(stock_list)
            
            self.logger.info(f"Stock list update completed. Total {inserted_count} stocks updated")
            return inserted_count
            
        except Exception as e:
            self.logger.error(f"Error updating stock list: {e}")
            return 0
    
    def update_daily_data(self, ts_codes: List[str] = None, force_full: bool = False) -> Dict:
        stats = {
            'total_stocks': 0,
            'success_stocks': 0,
            'failed_stocks': 0,
            'total_records': 0,
            'full_updates': 0,
            'incremental_updates': 0
        }
        
        try:
            self.logger.info("Starting to update daily data...")
            
            if ts_codes is None:
                stocks = self.db_manager.get_stock_list()
                ts_codes = [stock['ts_code'] for stock in stocks]
            
            stats['total_stocks'] = len(ts_codes)
            
            trade_dates = self.tushare_client.get_last_n_trade_dates(TRADING_DAYS_COUNT)
            
            if not trade_dates:
                self.logger.error("Failed to get trade dates")
                return stats
            
            start_date = trade_dates[0]
            end_date = trade_dates[-1]
            
            self.logger.info(f"Updating data from {start_date} to {end_date} ({len(trade_dates)} trade dates)")
            
            for i, ts_code in enumerate(ts_codes):
                try:
                    self.logger.info(f"Processing {ts_code} ({i+1}/{len(ts_codes)})")
                    
                    latest_date = self.db_manager.get_latest_trade_date(ts_code)
                    
                    if force_full or not latest_date:
                        df = self.tushare_client.get_daily_data(ts_code, start_date, end_date)
                        stats['full_updates'] += 1
                    else:
                        if latest_date >= end_date:
                            self.logger.debug(f"{ts_code} already up to date")
                            stats['success_stocks'] += 1
                            continue
                        
                        df = self.tushare_client.get_daily_data(ts_code, latest_date, end_date)
                        stats['incremental_updates'] += 1
                    
                    if not df.empty:
                        df = self._clean_data(df)
                        
                        daily_data = df.to_dict('records')
                        
                        inserted_count = self.db_manager.insert_daily_data(daily_data)
                        stats['total_records'] += inserted_count
                        stats['success_stocks'] += 1
                        
                        self.logger.info(f"Updated {inserted_count} records for {ts_code}")
                    else:
                        self.logger.warning(f"No data fetched for {ts_code}")
                        stats['failed_stocks'] += 1
                        
                except Exception as e:
                    self.logger.error(f"Error updating data for {ts_code}: {e}")
                    stats['failed_stocks'] += 1
            
            self.logger.info(f"Daily data update completed: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error in update_daily_data: {e}")
            return stats
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.dropna(subset=['open', 'high', 'low', 'close', 'vol'])
            
            df['open'] = pd.to_numeric(df['open'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['vol'] = pd.to_numeric(df['vol'], errors='coerce')
            
            df = df.dropna(subset=['open', 'high', 'low', 'close', 'vol'])
            
            df = df.sort_values('trade_date')
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error cleaning data: {e}")
            return df
    
    def get_stock_data(self, ts_code: str, days: int = None) -> pd.DataFrame:
        try:
            if days is None:
                days = TRADING_DAYS_COUNT
            
            trade_dates = self.tushare_client.get_last_n_trade_dates(days)
            
            if not trade_dates:
                self.logger.error("Failed to get trade dates")
                return pd.DataFrame()
            
            start_date = trade_dates[0]
            end_date = trade_dates[-1]
            
            daily_data = self.db_manager.get_daily_data(ts_code, start_date, end_date)
            
            if daily_data:
                df = pd.DataFrame(daily_data)
                return df
            else:
                df = self.tushare_client.get_daily_data(ts_code, start_date, end_date)
                
                if not df.empty:
                    df = self._clean_data(df)
                    daily_data = df.to_dict('records')
                    self.db_manager.insert_daily_data(daily_data)
                    return df
                else:
                    return pd.DataFrame()
                    
        except Exception as e:
            self.logger.error(f"Error getting stock data for {ts_code}: {e}")
            return pd.DataFrame()
    
    def update_single_stock(self, ts_code: str) -> bool:
        try:
            self.logger.info(f"Updating data for {ts_code}")
            
            stats = self.update_daily_data([ts_code])
            
            return stats['success_stocks'] > 0
            
        except Exception as e:
            self.logger.error(f"Error updating single stock {ts_code}: {e}")
            return False
    
    def get_update_summary(self) -> Dict:
        try:
            stock_count = self.db_manager.get_data_count('stock_info')
            daily_count = self.db_manager.get_data_count('daily_data')
            tech_count = self.db_manager.get_data_count('technical_indicators')
            selection_count = self.db_manager.get_data_count('stock_selection')
            
            summary = {
                'stock_info_count': stock_count,
                'daily_data_count': daily_count,
                'technical_indicators_count': tech_count,
                'stock_selection_count': selection_count,
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting update summary: {e}")
            return {}
