import tushare as ts
import pandas as pd
import logging
import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import TUSHARE_TOKEN, REQUEST_INTERVAL, MAX_RETRY_TIMES


class TushareClient:
    def __init__(self, token: str = None):
        self.token = token or TUSHARE_TOKEN
        self.pro = ts.pro_api(self.token)
        self.logger = logging.getLogger(__name__)
        self._last_request_time = 0
    
    def _rate_limit(self):
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        
        if time_since_last_request < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - time_since_last_request)
        
        self._last_request_time = time.time()
    
    def _retry_request(self, func, *args, **kwargs):
        for attempt in range(MAX_RETRY_TIMES):
            try:
                self._rate_limit()
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}/{MAX_RETRY_TIMES}): {e}")
                if attempt == MAX_RETRY_TIMES - 1:
                    raise
                time.sleep(2 ** attempt)
        
        return None
    
    def get_stock_list(self, exchange: str = None, list_status: str = 'L') -> pd.DataFrame:
        try:
            self.logger.info("Fetching stock list...")
            
            df = self._retry_request(
                self.pro.stock_basic,
                exchange=exchange,
                list_status=list_status,
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            
            if df is not None and not df.empty:
                self.logger.info(f"Fetched {len(df)} stocks")
                return df
            else:
                self.logger.warning("No stock data returned")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching stock list: {e}")
            return pd.DataFrame()
    
    def get_trade_calendar(self, start_date: str, end_date: str, exchange: str = 'SSE') -> List[str]:
        try:
            self.logger.info(f"Fetching trade calendar from {start_date} to {end_date}")
            
            df = self._retry_request(
                self.pro.trade_cal,
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                is_open='1'
            )
            
            if df is not None and not df.empty:
                trade_dates = df['cal_date'].tolist()
                self.logger.info(f"Fetched {len(trade_dates)} trade dates")
                return sorted(trade_dates)
            else:
                self.logger.warning("No trade calendar data returned")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching trade calendar: {e}")
            return []
    
    def get_daily_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            self.logger.debug(f"Fetching daily data for {ts_code} from {start_date} to {end_date}")
            
            df = self._retry_request(
                self.pro.daily,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df is not None and not df.empty:
                self.logger.debug(f"Fetched {len(df)} daily records for {ts_code}")
                return df
            else:
                self.logger.debug(f"No daily data returned for {ts_code}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching daily data for {ts_code}: {e}")
            return pd.DataFrame()
    
    def get_daily_data_batch(self, ts_codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        all_data = []
        
        for i, ts_code in enumerate(ts_codes):
            self.logger.info(f"Fetching data for {ts_code} ({i+1}/{len(ts_codes)})")
            
            df = self.get_daily_data(ts_code, start_date, end_date)
            
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"Total fetched {len(result_df)} daily records")
            return result_df
        else:
            return pd.DataFrame()
    
    def get_realtime_quotes(self, ts_codes: List[str]) -> pd.DataFrame:
        try:
            self.logger.info(f"Fetching realtime quotes for {len(ts_codes)} stocks")
            
            df = self._retry_request(
                ts.get_realtime_quotes,
                ts_codes
            )
            
            if df is not None and not df.empty:
                self.logger.info(f"Fetched realtime quotes for {len(df)} stocks")
                return df
            else:
                self.logger.warning("No realtime quotes data returned")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching realtime quotes: {e}")
            return pd.DataFrame()
    
    def get_last_n_trade_dates(self, n: int = 150) -> List[str]:
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=n * 2)).strftime('%Y%m%d')
        
        trade_dates = self.get_trade_calendar(start_date, end_date)
        
        if len(trade_dates) >= n:
            return trade_dates[-n:]
        else:
            self.logger.warning(f"Only {len(trade_dates)} trade dates available, requested {n}")
            return trade_dates
    
    def get_stock_basic_info(self, ts_code: str) -> Optional[Dict]:
        try:
            df = self._retry_request(
                self.pro.stock_basic,
                ts_code=ts_code,
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            
            if df is not None and not df.empty:
                return df.iloc[0].to_dict()
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error fetching stock basic info for {ts_code}: {e}")
            return None
    
    def test_connection(self) -> bool:
        try:
            self.logger.info("Testing Tushare connection...")
            
            df = self._retry_request(
                self.pro.trade_cal,
                exchange='SSE',
                start_date='20240101',
                end_date='20240110',
                is_open='1'
            )
            
            if df is not None and not df.empty:
                self.logger.info("Tushare connection test successful")
                return True
            else:
                self.logger.warning("Tushare connection test failed: no data returned")
                return False
                
        except Exception as e:
            self.logger.error(f"Tushare connection test failed: {e}")
            return False
