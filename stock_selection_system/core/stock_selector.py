import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import DatabaseManager
from clients.tencent_client import TencentClient
from config.settings import (
    VOLUME_RATIO_THRESHOLD, 
    PRICE_CHANGE_MIN, 
    PRICE_CHANGE_MAX,
    MA89_PERIOD
)
from config.constants import BREAKTHROUGH_TYPE


class StockSelector:
    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager or DatabaseManager()
        self.tencent_client = TencentClient()
        self.logger = logging.getLogger(__name__)
    
    def select_stocks_realtime(self, trade_date: str = None) -> List[Dict]:
        try:
            if trade_date is None:
                trade_date = datetime.now().strftime('%Y%m%d')
            
            self.logger.info(f"Starting realtime stock selection for {trade_date}")
            
            stocks = self.db_manager.get_stock_list()
            if not stocks:
                self.logger.warning("No stocks found in database")
                return []
            
            ts_codes = [stock['ts_code'] for stock in stocks]
            self.logger.info(f"Total stocks to analyze: {len(ts_codes)}")
            
            self.logger.info("Step 1: Fetching realtime quotes from Tencent...")
            realtime_quotes = self.tencent_client.get_realtime_quotes(ts_codes)
            
            if not realtime_quotes:
                self.logger.warning("No realtime quotes returned")
                return []
            
            self.logger.info(f"Got {len(realtime_quotes)} realtime quotes")
            
            self.logger.info("Step 2: Filtering by price change (1.8% - 8%)...")
            filtered_quotes = self._filter_by_price_change(realtime_quotes)
            self.logger.info(f"After price change filter: {len(filtered_quotes)} stocks")
            
            self.logger.info("Step 3: Calculating realtime MA89 and volume ratio...")
            selected_stocks = []
            
            for quote in filtered_quotes:
                ts_code = quote['ts_code']
                
                historical_data = self._get_historical_closes(ts_code, MA89_PERIOD - 1)
                
                if len(historical_data['closes']) < MA89_PERIOD - 1:
                    continue
                
                ma89 = self._calculate_realtime_ma89(historical_data['closes'], quote['current_price'])
                
                volume_ratio = self._calculate_realtime_volume_ratio(
                    historical_data['volumes'], 
                    quote['vol']
                )
                
                if volume_ratio <= VOLUME_RATIO_THRESHOLD:
                    continue
                
                if not self._check_breakthrough(quote, ma89):
                    continue
                
                selection_data = self._create_selection_data(quote, trade_date, ma89, volume_ratio)
                
                if selection_data:
                    selected_stocks.append(selection_data)
                    self.db_manager.insert_stock_selection(selection_data)
                    
                    self._save_today_ma89(ts_code, trade_date, quote, ma89, volume_ratio)
            
            self.logger.info(f"Selected {len(selected_stocks)} stocks for {trade_date}")
            return selected_stocks
            
        except Exception as e:
            self.logger.error(f"Error in realtime stock selection: {e}")
            return []
    
    def _filter_by_price_change(self, quotes: List[Dict]) -> List[Dict]:
        filtered = []
        for quote in quotes:
            pct_chg = quote.get('pct_chg', 0)
            if PRICE_CHANGE_MIN <= pct_chg <= PRICE_CHANGE_MAX:
                filtered.append(quote)
        return filtered
    
    def _get_historical_closes(self, ts_code: str, days: int) -> Dict:
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT trade_date, close, vol 
                FROM daily_data 
                WHERE ts_code = ? 
                ORDER BY trade_date DESC 
                LIMIT ?
            ''', (ts_code, days))
            rows = cursor.fetchall()
        
        closes = [row['close'] for row in reversed(rows)]
        volumes = [row['vol'] for row in reversed(rows)]
        
        return {'closes': closes, 'volumes': volumes}
    
    def _calculate_realtime_ma89(self, historical_closes: List[float], current_price: float) -> float:
        all_closes = historical_closes + [current_price]
        return np.mean(all_closes)
    
    def _calculate_realtime_volume_ratio(self, historical_volumes: List[float], current_volume: float) -> float:
        if len(historical_volumes) < 5:
            return 0
        
        recent_5_volumes = historical_volumes[-5:]
        avg_volume = np.mean(recent_5_volumes)
        
        if avg_volume == 0:
            return 0
        
        return current_volume / avg_volume
    
    def _check_breakthrough(self, quote: Dict, ma89: float) -> bool:
        low = quote.get('low', 0)
        current_price = quote.get('current_price', 0)
        
        if low <= 0 or current_price <= 0 or ma89 <= 0:
            return False
        
        if low >= ma89:
            return False
        
        if current_price <= ma89:
            return False
        
        return True
    
    def _create_selection_data(self, quote: Dict, trade_date: str, ma89: float, volume_ratio: float) -> Optional[Dict]:
        try:
            selection_reason = self._generate_selection_reason(quote, ma89, volume_ratio)
            
            selection_data = {
                'ts_code': quote['ts_code'],
                'trade_date': trade_date,
                'selection_time': datetime.now().strftime('%H:%M'),
                'open_price': quote.get('open'),
                'current_price': quote['current_price'],
                'ma89_value': ma89,
                'volume_ratio': volume_ratio,
                'pct_chg': quote['pct_chg'],
                'breakthrough_type': BREAKTHROUGH_TYPE['UP'],
                'selection_reason': selection_reason
            }
            
            return selection_data
            
        except Exception as e:
            self.logger.error(f"Error creating selection data: {e}")
            return None
    
    def _generate_selection_reason(self, quote: Dict, ma89: float, volume_ratio: float) -> str:
        reasons = []
        
        if volume_ratio > VOLUME_RATIO_THRESHOLD:
            reasons.append(f"放量上涨(量比{volume_ratio:.2f})")
        
        low = quote.get('low', 0)
        current_price = quote.get('current_price', 0)
        if low < ma89 and current_price > ma89:
            reasons.append("突破89日线")
        
        pct_chg = quote.get('pct_chg', 0)
        if PRICE_CHANGE_MIN <= pct_chg <= PRICE_CHANGE_MAX:
            reasons.append(f"涨幅适中({pct_chg:.2f}%)")
        
        return "，".join(reasons) if reasons else "符合选股条件"
    
    def _save_today_ma89(self, ts_code: str, trade_date: str, quote: Dict, ma89: float, volume_ratio: float):
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT id FROM daily_data WHERE ts_code = ? AND trade_date = ?
                ''', (ts_code, trade_date))
                
                if cursor.fetchone():
                    cursor.execute('''
                        UPDATE daily_data 
                        SET close = ?, high = ?, low = ?, vol = ?, amount = ?,
                            pct_chg = ?, ma89 = ?, volume_ratio = ?
                        WHERE ts_code = ? AND trade_date = ?
                    ''', (
                        quote['current_price'], quote['high'], quote['low'],
                        quote['vol'], quote['amount'], quote['pct_chg'],
                        ma89, volume_ratio, ts_code, trade_date
                    ))
                else:
                    cursor.execute('''
                        INSERT INTO daily_data 
                        (ts_code, trade_date, open, high, low, close, pre_close,
                         change, pct_chg, vol, amount, ma89, volume_ratio)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        ts_code, trade_date, quote['open'], quote['high'], quote['low'],
                        quote['current_price'], quote['pre_close'], quote.get('change', 0),
                        quote['pct_chg'], quote['vol'], quote['amount'], ma89, volume_ratio
                    ))
                
                conn.commit()
                self.logger.debug(f"Saved MA89 for {ts_code} on {trade_date}")
                
        except Exception as e:
            self.logger.error(f"Error saving today's MA89 for {ts_code}: {e}")
    
    def select_stocks_noon(self, trade_date: str = None) -> List[Dict]:
        return self.select_stocks_realtime(trade_date)
    
    def get_selection_results(self, trade_date: str = None) -> List[Dict]:
        try:
            if trade_date is None:
                trade_date = datetime.now().strftime('%Y%m%d')
            
            results = self.db_manager.get_stock_selection(trade_date)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error getting selection results: {e}")
            return []
    
    def analyze_selection_performance(self, days: int = 30) -> Dict:
        try:
            self.logger.info(f"Analyzing selection performance for last {days} days")
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT ss.*, dd.close as next_day_close
                    FROM stock_selection ss
                    LEFT JOIN daily_data dd ON ss.ts_code = dd.ts_code 
                        AND dd.trade_date = date(ss.trade_date, '+1 day')
                    WHERE ss.trade_date >= date('now', ?)
                    ORDER BY ss.trade_date DESC
                ''', (f'-{days} days',))
                
                rows = cursor.fetchall()
                selections = [dict(row) for row in rows]
            
            if not selections:
                return {'total_selections': 0}
            
            total_selections = len(selections)
            profitable_selections = sum(1 for s in selections if s.get('next_day_close', 0) > s.get('current_price', 0))
            
            avg_return = 0
            if selections:
                returns = []
                for s in selections:
                    if s.get('next_day_close') and s.get('current_price'):
                        ret = (s['next_day_close'] - s['current_price']) / s['current_price'] * 100
                        returns.append(ret)
                
                if returns:
                    avg_return = sum(returns) / len(returns)
            
            performance = {
                'total_selections': total_selections,
                'profitable_selections': profitable_selections,
                'win_rate': profitable_selections / total_selections * 100 if total_selections > 0 else 0,
                'avg_return': avg_return,
                'analysis_period': f'{days} days'
            }
            
            self.logger.info(f"Performance analysis: {performance}")
            return performance
            
        except Exception as e:
            self.logger.error(f"Error analyzing selection performance: {e}")
            return {'total_selections': 0}
    
    def export_selection_results(self, trade_date: str = None, output_file: str = None) -> bool:
        try:
            results = self.get_selection_results(trade_date)
            
            if not results:
                self.logger.warning("No selection results to export")
                return False
            
            df = pd.DataFrame(results)
            
            if output_file is None:
                if trade_date is None:
                    trade_date = datetime.now().strftime('%Y%m%d')
                output_file = f"selection_results_{trade_date}.csv"
            
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"Exported {len(results)} selection results to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting selection results: {e}")
            return False
    
    def get_top_selections(self, limit: int = 10, trade_date: str = None) -> List[Dict]:
        try:
            if trade_date is None:
                trade_date = datetime.now().strftime('%Y%m%d')
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT ss.*, si.name, si.industry
                    FROM stock_selection ss
                    LEFT JOIN stock_info si ON ss.ts_code = si.ts_code
                    WHERE ss.trade_date = ?
                    ORDER BY ss.pct_chg DESC
                    LIMIT ?
                ''', (trade_date, limit))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting top selections: {e}")
            return []
