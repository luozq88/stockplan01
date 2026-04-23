import sqlite3
import logging
from typing import List, Dict, Optional, Any
from contextlib import contextmanager
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATABASE_PATH, BATCH_INSERT_SIZE


class DatabaseManager:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DATABASE_PATH)
        self.logger = logging.getLogger(__name__)
        self._init_database()
    
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def _init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock_info (
                    ts_code TEXT PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    area TEXT,
                    industry TEXT,
                    list_date TEXT,
                    updated_at TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT,
                    trade_date TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    pre_close REAL,
                    change REAL,
                    pct_chg REAL,
                    vol REAL,
                    amount REAL,
                    ma89 REAL,
                    volume_ratio REAL,
                    is_breakthrough INTEGER,
                    UNIQUE(ts_code, trade_date)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS technical_indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT,
                    trade_date TEXT,
                    ma5 REAL,
                    ma10 REAL,
                    ma20 REAL,
                    ma30 REAL,
                    ma60 REAL,
                    ma89 REAL,
                    ma120 REAL,
                    ma250 REAL,
                    volume_avg_5 REAL,
                    volume_avg_10 REAL,
                    volume_ratio REAL,
                    rsi_6 REAL,
                    rsi_12 REAL,
                    rsi_24 REAL,
                    updated_at TEXT,
                    UNIQUE(ts_code, trade_date)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock_selection (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT,
                    trade_date TEXT,
                    selection_time TEXT,
                    open_price REAL,
                    current_price REAL,
                    ma89_value REAL,
                    volume_ratio REAL,
                    pct_chg REAL,
                    breakthrough_type INTEGER,
                    selection_reason TEXT,
                    created_at TEXT
                )
            ''')
            
            self._create_indexes(cursor)
            
            conn.commit()
            self.logger.info("Database initialized successfully")
    
    def _create_indexes(self, cursor):
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_daily_ts_code ON daily_data(ts_code)",
            "CREATE INDEX IF NOT EXISTS idx_daily_trade_date ON daily_data(trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_daily_code_date ON daily_data(ts_code, trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_daily_ma89 ON daily_data(ma89)",
            "CREATE INDEX IF NOT EXISTS idx_daily_breakthrough ON daily_data(is_breakthrough)",
            "CREATE INDEX IF NOT EXISTS idx_tech_ts_code ON technical_indicators(ts_code)",
            "CREATE INDEX IF NOT EXISTS idx_tech_trade_date ON technical_indicators(trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_tech_ma89 ON technical_indicators(ma89)",
            "CREATE INDEX IF NOT EXISTS idx_selection_date ON stock_selection(trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_selection_ts_code ON stock_selection(ts_code)",
            "CREATE INDEX IF NOT EXISTS idx_selection_breakthrough ON stock_selection(breakthrough_type)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
    
    def insert_stock_info(self, stock_data: List[Dict]) -> int:
        if not stock_data:
            return 0
        
        inserted_count = 0
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for stock in stock_data:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO stock_info 
                        (ts_code, symbol, name, area, industry, list_date, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        stock['ts_code'],
                        stock['symbol'],
                        stock['name'],
                        stock.get('area', ''),
                        stock.get('industry', ''),
                        stock.get('list_date', ''),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))
                    inserted_count += 1
                except Exception as e:
                    self.logger.error(f"Error inserting stock {stock.get('ts_code')}: {e}")
            
            conn.commit()
        
        self.logger.info(f"Inserted {inserted_count} stock info records")
        return inserted_count
    
    def insert_daily_data(self, daily_data: List[Dict]) -> int:
        if not daily_data:
            return 0
        
        inserted_count = 0
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for i in range(0, len(daily_data), BATCH_INSERT_SIZE):
                batch = daily_data[i:i + BATCH_INSERT_SIZE]
                
                for data in batch:
                    try:
                        cursor.execute('''
                            INSERT OR REPLACE INTO daily_data 
                            (ts_code, trade_date, open, high, low, close, pre_close, 
                             change, pct_chg, vol, amount, ma89, volume_ratio, is_breakthrough)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            data['ts_code'],
                            data['trade_date'],
                            data['open'],
                            data['high'],
                            data['low'],
                            data['close'],
                            data['pre_close'],
                            data['change'],
                            data['pct_chg'],
                            data['vol'],
                            data['amount'],
                            data.get('ma89'),
                            data.get('volume_ratio'),
                            data.get('is_breakthrough', 0)
                        ))
                        inserted_count += 1
                    except Exception as e:
                        self.logger.error(f"Error inserting daily data {data.get('ts_code')} {data.get('trade_date')}: {e}")
                
                conn.commit()
                self.logger.info(f"Inserted batch {i // BATCH_INSERT_SIZE + 1}: {len(batch)} records")
        
        self.logger.info(f"Total inserted {inserted_count} daily data records")
        return inserted_count
    
    def insert_technical_indicators(self, indicators: List[Dict]) -> int:
        if not indicators:
            return 0
        
        inserted_count = 0
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for indicator in indicators:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO technical_indicators
                        (ts_code, trade_date, ma5, ma10, ma20, ma30, ma60, ma89, ma120, ma250,
                         volume_avg_5, volume_avg_10, volume_ratio, rsi_6, rsi_12, rsi_24, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        indicator['ts_code'],
                        indicator['trade_date'],
                        indicator.get('ma5'),
                        indicator.get('ma10'),
                        indicator.get('ma20'),
                        indicator.get('ma30'),
                        indicator.get('ma60'),
                        indicator.get('ma89'),
                        indicator.get('ma120'),
                        indicator.get('ma250'),
                        indicator.get('volume_avg_5'),
                        indicator.get('volume_avg_10'),
                        indicator.get('volume_ratio'),
                        indicator.get('rsi_6'),
                        indicator.get('rsi_12'),
                        indicator.get('rsi_24'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))
                    inserted_count += 1
                except Exception as e:
                    self.logger.error(f"Error inserting technical indicator {indicator.get('ts_code')} {indicator.get('trade_date')}: {e}")
            
            conn.commit()
        
        self.logger.info(f"Inserted {inserted_count} technical indicator records")
        return inserted_count
    
    def insert_stock_selection(self, selection_data: Dict) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    INSERT INTO stock_selection
                    (ts_code, trade_date, selection_time, open_price, current_price, 
                     ma89_value, volume_ratio, pct_chg, breakthrough_type, selection_reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    selection_data['ts_code'],
                    selection_data['trade_date'],
                    selection_data['selection_time'],
                    selection_data['open_price'],
                    selection_data['current_price'],
                    selection_data['ma89_value'],
                    selection_data['volume_ratio'],
                    selection_data['pct_chg'],
                    selection_data['breakthrough_type'],
                    selection_data['selection_reason'],
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
                conn.commit()
                self.logger.info(f"Inserted stock selection for {selection_data['ts_code']}")
                return 1
            except Exception as e:
                self.logger.error(f"Error inserting stock selection: {e}")
                return 0
    
    def get_stock_list(self) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM stock_info")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def get_latest_trade_date(self, ts_code: str) -> Optional[str]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT MAX(trade_date) as latest_date FROM daily_data WHERE ts_code = ?",
                (ts_code,)
            )
            result = cursor.fetchone()
            return result['latest_date'] if result else None
    
    def get_daily_data(self, ts_code: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if start_date and end_date:
                cursor.execute('''
                    SELECT * FROM daily_data 
                    WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
                    ORDER BY trade_date
                ''', (ts_code, start_date, end_date))
            else:
                cursor.execute('''
                    SELECT * FROM daily_data 
                    WHERE ts_code = ?
                    ORDER BY trade_date
                ''', (ts_code,))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def get_technical_indicators(self, ts_code: str, trade_date: str = None) -> Optional[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if trade_date:
                cursor.execute('''
                    SELECT * FROM technical_indicators 
                    WHERE ts_code = ? AND trade_date = ?
                ''', (ts_code, trade_date))
            else:
                cursor.execute('''
                    SELECT * FROM technical_indicators 
                    WHERE ts_code = ?
                    ORDER BY trade_date DESC LIMIT 1
                ''', (ts_code,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_stock_selection(self, trade_date: str = None) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if trade_date:
                cursor.execute('''
                    SELECT * FROM stock_selection WHERE trade_date = ?
                    ORDER BY created_at
                ''', (trade_date,))
            else:
                cursor.execute('''
                    SELECT * FROM stock_selection 
                    ORDER BY trade_date DESC, created_at
                ''')
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def update_daily_data_indicators(self, ts_code: str, trade_date: str, 
                                     ma89: float = None, volume_ratio: float = None, 
                                     is_breakthrough: int = None) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    UPDATE daily_data 
                    SET ma89 = ?, volume_ratio = ?, is_breakthrough = ?
                    WHERE ts_code = ? AND trade_date = ?
                ''', (ma89, volume_ratio, is_breakthrough, ts_code, trade_date))
                
                conn.commit()
                return True
            except Exception as e:
                self.logger.error(f"Error updating daily data indicators: {e}")
                return False
    
    def get_data_count(self, table_name: str) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            result = cursor.fetchone()
            return result['count'] if result else 0
    
    def get_update_summary(self) -> Dict:
        try:
            stock_count = self.get_data_count('stock_info')
            daily_count = self.get_data_count('daily_data')
            tech_count = self.get_data_count('technical_indicators')
            selection_count = self.get_data_count('stock_selection')
            
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
