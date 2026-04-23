import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import DatabaseManager
from config.settings import MA89_PERIOD


class TechnicalAnalysis:
    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager or DatabaseManager()
        self.logger = logging.getLogger(__name__)
    
    def calculate_ma(self, data: pd.DataFrame, period: int) -> pd.Series:
        try:
            return data['close'].rolling(window=period, min_periods=period).mean()
        except Exception as e:
            self.logger.error(f"Error calculating MA{period}: {e}")
            return pd.Series()
    
    def calculate_volume_ratio(self, data: pd.DataFrame, period: int = 5) -> pd.Series:
        try:
            volume_avg = data['vol'].rolling(window=period, min_periods=period).mean()
            volume_ratio = data['vol'] / volume_avg
            return volume_ratio
        except Exception as e:
            self.logger.error(f"Error calculating volume ratio: {e}")
            return pd.Series()
    
    def calculate_rsi(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        try:
            delta = data['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            return rsi
        except Exception as e:
            self.logger.error(f"Error calculating RSI{period}: {e}")
            return pd.Series()
    
    def calculate_all_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            df = data.copy()
            
            df['ma5'] = self.calculate_ma(df, 5)
            df['ma10'] = self.calculate_ma(df, 10)
            df['ma20'] = self.calculate_ma(df, 20)
            df['ma30'] = self.calculate_ma(df, 30)
            df['ma60'] = self.calculate_ma(df, 60)
            df['ma89'] = self.calculate_ma(df, 89)
            df['ma120'] = self.calculate_ma(df, 120)
            df['ma250'] = self.calculate_ma(df, 250)
            
            df['volume_avg_5'] = df['vol'].rolling(window=5, min_periods=5).mean()
            df['volume_avg_10'] = df['vol'].rolling(window=10, min_periods=10).mean()
            df['volume_ratio'] = self.calculate_volume_ratio(df, 5)
            
            df['rsi_6'] = self.calculate_rsi(df, 6)
            df['rsi_12'] = self.calculate_rsi(df, 12)
            df['rsi_24'] = self.calculate_rsi(df, 24)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error calculating all indicators: {e}")
            return data
    
    def detect_breakthrough(self, data: pd.DataFrame, ma_period: int = 89) -> pd.DataFrame:
        try:
            df = data.copy()
            
            ma_col = f'ma{ma_period}'
            
            if ma_col not in df.columns:
                df[ma_col] = self.calculate_ma(df, ma_period)
            
            df['is_breakthrough'] = 0
            
            for i in range(1, len(df)):
                if pd.notna(df[ma_col].iloc[i]):
                    prev_low = df['low'].iloc[i-1]
                    curr_close = df['close'].iloc[i]
                    ma_value = df[ma_col].iloc[i]
                    
                    if prev_low < ma_value and curr_close > ma_value:
                        df.at[df.index[i], 'is_breakthrough'] = 1
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error detecting breakthrough: {e}")
            return data
    
    def update_technical_indicators(self, ts_code: str) -> bool:
        try:
            self.logger.info(f"Updating technical indicators for {ts_code}")
            
            daily_data = self.db_manager.get_daily_data(ts_code)
            
            if not daily_data:
                self.logger.warning(f"No daily data found for {ts_code}")
                return False
            
            df = pd.DataFrame(daily_data)
            
            df = self.calculate_all_indicators(df)
            
            df = self.detect_breakthrough(df, 89)
            
            indicators = []
            for _, row in df.iterrows():
                if pd.notna(row['ma89']):
                    indicator = {
                        'ts_code': row['ts_code'],
                        'trade_date': row['trade_date'],
                        'ma5': row.get('ma5'),
                        'ma10': row.get('ma10'),
                        'ma20': row.get('ma20'),
                        'ma30': row.get('ma30'),
                        'ma60': row.get('ma60'),
                        'ma89': row.get('ma89'),
                        'ma120': row.get('ma120'),
                        'ma250': row.get('ma250'),
                        'volume_avg_5': row.get('volume_avg_5'),
                        'volume_avg_10': row.get('volume_avg_10'),
                        'volume_ratio': row.get('volume_ratio'),
                        'rsi_6': row.get('rsi_6'),
                        'rsi_12': row.get('rsi_12'),
                        'rsi_24': row.get('rsi_24')
                    }
                    indicators.append(indicator)
            
            if indicators:
                inserted_count = self.db_manager.insert_technical_indicators(indicators)
                self.logger.info(f"Updated {inserted_count} technical indicators for {ts_code}")
                return True
            else:
                self.logger.warning(f"No valid technical indicators to update for {ts_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error updating technical indicators for {ts_code}: {e}")
            return False
    
    def update_daily_data_indicators(self, ts_code: str) -> bool:
        try:
            self.logger.info(f"Updating daily data indicators for {ts_code}")
            
            daily_data = self.db_manager.get_daily_data(ts_code)
            
            if not daily_data:
                self.logger.warning(f"No daily data found for {ts_code}")
                return False
            
            df = pd.DataFrame(daily_data)
            
            df['ma89'] = self.calculate_ma(df, 89)
            df['volume_ratio'] = self.calculate_volume_ratio(df, 5)
            
            df = self.detect_breakthrough(df, 89)
            
            update_count = 0
            for _, row in df.iterrows():
                if pd.notna(row['ma89']):
                    success = self.db_manager.update_daily_data_indicators(
                        ts_code=row['ts_code'],
                        trade_date=row['trade_date'],
                        ma89=row['ma89'],
                        volume_ratio=row.get('volume_ratio'),
                        is_breakthrough=row.get('is_breakthrough', 0)
                    )
                    if success:
                        update_count += 1
            
            self.logger.info(f"Updated {update_count} daily data indicators for {ts_code}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating daily data indicators for {ts_code}: {e}")
            return False
    
    def batch_update_indicators(self, ts_codes: List[str] = None) -> Dict:
        stats = {
            'total_stocks': 0,
            'success_stocks': 0,
            'failed_stocks': 0
        }
        
        try:
            if ts_codes is None:
                stocks = self.db_manager.get_stock_list()
                ts_codes = [stock['ts_code'] for stock in stocks]
            
            stats['total_stocks'] = len(ts_codes)
            
            for i, ts_code in enumerate(ts_codes):
                self.logger.info(f"Processing {ts_code} ({i+1}/{len(ts_codes)})")
                
                success = self.update_technical_indicators(ts_code)
                
                if success:
                    stats['success_stocks'] += 1
                else:
                    stats['failed_stocks'] += 1
            
            self.logger.info(f"Batch update completed: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error in batch update: {e}")
            return stats
    
    def get_ma89_breakthrough_stocks(self, trade_date: str) -> List[Dict]:
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT dd.*, si.name, si.industry
                    FROM daily_data dd
                    LEFT JOIN stock_info si ON dd.ts_code = si.ts_code
                    WHERE dd.trade_date = ? AND dd.is_breakthrough = 1
                    ORDER BY dd.pct_chg DESC
                ''', (trade_date,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting MA89 breakthrough stocks: {e}")
            return []
