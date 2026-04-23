import requests
import logging
import re
from typing import List, Dict, Optional
import time
from datetime import datetime


class TencentClient:
    def __init__(self):
        self.base_url = "http://qt.gtimg.cn/q="
        self.logger = logging.getLogger(__name__)
        self._last_request_time = 0
        self.request_interval = 0.1
    
    def _rate_limit(self):
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        
        if time_since_last_request < self.request_interval:
            time.sleep(self.request_interval - time_since_last_request)
        
        self._last_request_time = time.time()
    
    def _convert_ts_code_to_tencent(self, ts_code: str) -> str:
        if '.' in ts_code:
            code, market = ts_code.split('.')
            if market == 'SH':
                return f"sh{code}"
            elif market == 'SZ':
                return f"sz{code}"
        return ts_code
    
    def _convert_tencent_to_ts_code(self, tencent_code: str) -> str:
        if tencent_code.startswith('sh'):
            return f"{tencent_code[2:]}.SH"
        elif tencent_code.startswith('sz'):
            return f"{tencent_code[2:]}.SZ"
        return tencent_code
    
    def _parse_quote_data(self, data_str: str, tencent_code: str) -> Optional[Dict]:
        try:
            pattern = f'v_{tencent_code}="([^"]*)"'
            match = re.search(pattern, data_str)
            if not match:
                return None

            values = match.group(1).split('~')
            
            if len(values) < 45:
                return None
            
            quote = {
                'ts_code': self._convert_tencent_to_ts_code(tencent_code),
                'name': values[1],
                'open': float(values[5]) if values[5] else 0,
                'pre_close': float(values[4]) if values[4] else 0,
                'current_price': float(values[3]) if values[3] else 0,
                'high': float(values[33]) if values[33] else 0,
                'low': float(values[34]) if values[34] else 0,
                'vol': float(values[6]) if values[6] else 0,
                'amount': float(values[37]) if values[37] else 0,
                'pct_chg': float(values[32]) if values[32] else 0,
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            if quote['pre_close'] > 0:
                quote['change'] = quote['current_price'] - quote['pre_close']
            
            return quote
            
        except Exception as e:
            self.logger.error(f"Error parsing quote data for {tencent_code}: {e}")
            return None
    
    def get_realtime_quotes(self, ts_codes: List[str], batch_size: int = 500) -> List[Dict]:
        all_quotes = []
        
        try:
            tencent_codes = [self._convert_ts_code_to_tencent(code) for code in ts_codes]
            
            for i in range(0, len(tencent_codes), batch_size):
                batch = tencent_codes[i:i + batch_size]
                batch_ts_codes = ts_codes[i:i + batch_size]
                
                self._rate_limit()
                
                codes_str = ','.join(batch)
                url = f"{self.base_url}{codes_str}"
                
                self.logger.info(f"Fetching realtime quotes batch {i // batch_size + 1}: {len(batch)} stocks")
                
                response = requests.get(url, timeout=10)
                response.encoding = 'gbk'
                
                if response.status_code == 200:
                    data_str = response.text
                    
                    for j, tencent_code in enumerate(batch):
                        quote = self._parse_quote_data(data_str, tencent_code)
                        if quote:
                            all_quotes.append(quote)
                else:
                    self.logger.error(f"Failed to fetch quotes: HTTP {response.status_code}")
            
            self.logger.info(f"Total fetched {len(all_quotes)} realtime quotes")
            return all_quotes
            
        except Exception as e:
            self.logger.error(f"Error fetching realtime quotes: {e}")
            return all_quotes
    
    def get_single_quote(self, ts_code: str) -> Optional[Dict]:
        quotes = self.get_realtime_quotes([ts_code])
        return quotes[0] if quotes else None
    
    def test_connection(self) -> bool:
        try:
            self.logger.info("Testing Tencent API connection...")
            
            test_code = 'sh600000'
            url = f"{self.base_url}{test_code}"
            
            response = requests.get(url, timeout=10)
            response.encoding = 'gbk'
            
            if response.status_code == 200 and 'v_' in response.text:
                self.logger.info("Tencent API connection test successful")
                return True
            else:
                self.logger.warning("Tencent API connection test failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Tencent API connection test failed: {e}")
            return False
