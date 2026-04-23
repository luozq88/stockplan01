import logging
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import DatabaseManager
from core.technical_analysis import TechnicalAnalysis
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
        self.technical_analysis = TechnicalAnalysis(db_manager)
        self.logger = logging.getLogger(__name__)
    
    def select_stocks_noon(self, trade_date: str = None) -> List[Dict]:
        try:
            if trade_date is None:
                trade_date = datetime.now().strftime('%Y%m%d')
            
            self.logger.info(f"Starting noon stock selection for {trade_date}")
            
            selected_stocks = []
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT dd.*, si.name, si.industry
                    FROM daily_data dd
                    LEFT JOIN stock_info si ON dd.ts_code = si.ts_code
                    WHERE dd.trade_date = ?
                    ORDER BY dd.pct_chg DESC
                ''', (trade_date,))
                
                rows = cursor.fetchall()
                all_stocks = [dict(row) for row in rows]
            
            self.logger.info(f"Found {len(all_stocks)} stocks for {trade_date}")
            
            for stock in all_stocks:
                if self._check_selection_criteria(stock):
                    selection_data = self._create_selection_data(stock, trade_date)
                    
                    if selection_data:
                        selected_stocks.append(selection_data)
                        self.db_manager.insert_stock_selection(selection_data)
            
            self.logger.info(f"Selected {len(selected_stocks)} stocks for {trade_date}")
            
            return selected_stocks
            
        except Exception as e:
            self.logger.error(f"Error in noon stock selection: {e}")
            return []
    
    def _check_selection_criteria(self, stock: Dict) -> bool:
        try:
            if not stock.get('pct_chg') or stock['pct_chg'] <= PRICE_CHANGE_MIN:
                return False
            
            if stock['pct_chg'] >= PRICE_CHANGE_MAX:
                return False
            
            if not stock.get('volume_ratio') or stock['volume_ratio'] <= VOLUME_RATIO_THRESHOLD:
                return False
            
            if not stock.get('ma89'):
                return False
            
            if stock.get('low') is None or stock.get('close') is None:
                return False
            
            if stock['low'] >= stock['ma89']:
                return False
            
            if stock['close'] <= stock['ma89']:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking selection criteria: {e}")
            return False
    
    def _create_selection_data(self, stock: Dict, trade_date: str) -> Optional[Dict]:
        try:
            selection_reason = self._generate_selection_reason(stock)
            
            selection_data = {
                'ts_code': stock['ts_code'],
                'trade_date': trade_date,
                'selection_time': '11:30',
                'open_price': stock.get('open'),
                'current_price': stock['close'],
                'ma89_value': stock['ma89'],
                'volume_ratio': stock['volume_ratio'],
                'pct_chg': stock['pct_chg'],
                'breakthrough_type': BREAKTHROUGH_TYPE['UP'],
                'selection_reason': selection_reason
            }
            
            return selection_data
            
        except Exception as e:
            self.logger.error(f"Error creating selection data: {e}")
            return None
    
    def _generate_selection_reason(self, stock: Dict) -> str:
        reasons = []
        
        if stock.get('volume_ratio', 0) > VOLUME_RATIO_THRESHOLD:
            reasons.append(f"放量上涨(量比{stock['volume_ratio']:.2f})")
        
        if stock.get('low', 0) < stock.get('ma89', 0) and stock.get('close', 0) > stock.get('ma89', 0):
            reasons.append("突破89日线")
        
        if 0 < stock.get('pct_chg', 0) < PRICE_CHANGE_MAX:
            reasons.append(f"涨幅适中({stock['pct_chg']:.2f}%)")
        
        return "，".join(reasons) if reasons else "符合选股条件"
    
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
