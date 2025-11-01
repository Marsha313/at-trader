import requests
import time
import hmac
import hashlib
import urllib.parse
import math
from typing import Dict, List, Optional, Tuple
import json
import threading
from dataclasses import dataclass
import os
from dotenv import load_dotenv
from enum import Enum
import logging
import sys
from datetime import datetime

# 设置日志
def setup_logging():
    """设置日志配置"""
    # 创建logs目录
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # 生成日志文件名（带时间戳）
    log_filename = f"logs/market_maker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # 同时输出到控制台
        ]
    )
    
    return logging.getLogger(__name__)

# 初始化日志
logger = setup_logging()

# 加载环境变量
load_dotenv()

class TradingStrategy(Enum):
    MARKET_ONLY = "market_only"
    LIMIT_MARKET = "limit_market"
    BOTH = "both"

@dataclass
class OrderBook:
    bids: List[List[float]]
    asks: List[List[float]]
    update_time: float

@dataclass
class AccountBalance:
    free: float
    locked: float

class AsterDexClient:
    def __init__(self, api_key: str, secret_key: str, account_name: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.account_name = account_name
        self.base_url = os.getenv('BASE_URL', 'https://sapi.asterdex.com')
        self.symbol_precision_cache = {}
        # 初始化余额缓存为None，表示需要首次加载
        self._balance_cache = None
        self.logger = logging.getLogger(f"{__name__}.{account_name}")
        
    def _sign_request(self, params: Dict) -> str:
        """生成签名"""
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False) -> Dict:
        """发送API请求"""
        url = f"{self.base_url}{endpoint}"
        headers = {
            'X-MBX-APIKEY': self.api_key
        }
        
        if params is None:
            params = {}
            
        if signed:
            params['timestamp'] = int(time.time() * 1000)
            params['recvWindow'] = 5000
            params['signature'] = self._sign_request(params)
        
        try:
            if method == 'GET':
                response = requests.get(url, params=params, headers=headers, timeout=10)
            elif method == 'POST':
                response = requests.post(url, data=params, headers=headers, timeout=10)
            elif method == 'DELETE':
                response = requests.delete(url, data=params, headers=headers, timeout=10)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API请求错误 ({self.account_name}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"错误响应: {e.response.text}")
            return {'error': str(e)}
    
    def preload_symbol_precision(self, symbol: str) -> bool:
        """预加载交易对精度信息"""
        if symbol in self.symbol_precision_cache:
            return True
            
        # 默认步长
        default_tick_size = 0.00001
        default_step_size = 0.00001
        
        try:
            endpoint = "/api/v1/exchangeInfo"
            params = {'symbol': symbol}
            data = self._request('GET', endpoint, params)
            
            if 'symbols' in data and data['symbols']:
                symbol_data = data['symbols'][0]
                
                # 从过滤器获取步长信息
                for filter_obj in symbol_data.get('filters', []):
                    filter_type = filter_obj.get('filterType')
                    if filter_type == 'PRICE_FILTER':
                        default_tick_size = float(filter_obj.get('tickSize', '0.00001'))
                    elif filter_type == 'LOT_SIZE':
                        default_step_size = float(filter_obj.get('stepSize', '0.00001'))
                
                self.logger.info(f"📊 {symbol} 步长信息: 价格={default_tick_size}, 数量={default_step_size}")
                self.symbol_precision_cache[symbol] = (default_tick_size, default_step_size)
                return True
            else:
                self.logger.warning(f"⚠️ 无法获取 {symbol} 的交易对信息，使用默认步长")
                self.symbol_precision_cache[symbol] = (default_tick_size, default_step_size)
                return False
        
        except Exception as e:
            self.logger.error(f"获取交易对信息失败: {e}, 使用默认步长")
            self.symbol_precision_cache[symbol] = (default_tick_size, default_step_size)
            return False
    
    def get_symbol_precision(self, symbol: str) -> Tuple[float, float]:
        """获取交易对的步长信息（从缓存中）"""
        return (0.00001, 0.00001)
    
    def get_order_book(self, symbol: str, limit: int = 10) -> OrderBook:
        """获取订单簿"""
        endpoint = "/api/v1/depth"
        params = {
            'symbol': symbol,
            'limit': limit
        }
        data = self._request('GET', endpoint, params)
        
        if not data or 'bids' not in data:
            return OrderBook(bids=[], asks=[], update_time=time.time())
            
        bids = [[float(bid[0]), float(bid[1])] for bid in data.get('bids', [])]
        asks = [[float(ask[0]), float(ask[1])] for ask in data.get('asks', [])]
        
        return OrderBook(bids=bids, asks=asks, update_time=time.time())
    
    def create_order(self, symbol: str, side: str, order_type: str, 
                    quantity: float, price: Optional[float] = None,
                    newClientOrderId: Optional[str] = None) -> Dict:
        """创建订单 - 使用缓存的精度信息"""
        endpoint = "/api/v1/order"
        
        # 格式化数量
        formatted_quantity = round(math.floor(quantity / 0.01 )* 0.01,2)
        
        # 格式化价格（如果是限价单）
        formatted_price = None
        if price is not None and order_type != 'MARKET':
            formatted_price = round(price,5)
        
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': formatted_quantity
        }
        
        if formatted_price is not None:
            params['price'] = formatted_price
            params['timeInForce'] = 'GTC'
        
        if newClientOrderId:
            params['newClientOrderId'] = newClientOrderId
        
        self.logger.info(f"📤 发送订单请求:")
        self.logger.info(f"   交易对: {symbol}")
        self.logger.info(f"   方向: {side}")
        self.logger.info(f"   类型: {order_type}")
        self.logger.info(f"   数量: {quantity} -> {formatted_quantity}")
        if formatted_price:
            self.logger.info(f"   价格: {price} -> {formatted_price}")
        
        return self._request('POST', endpoint, params, signed=True)
    
    def cancel_order(self, symbol: str, order_id: int = None, origClientOrderId: str = None) -> Dict:
        """取消订单"""
        endpoint = "/api/v1/order"
        params = {'symbol': symbol}
        
        if order_id:
            params['orderId'] = order_id
        elif origClientOrderId:
            params['origClientOrderId'] = origClientOrderId
        else:
            return {'error': '必须提供orderId或origClientOrderId'}
            
        return self._request('DELETE', endpoint, params, signed=True)
    
    def get_order(self, symbol: str, order_id: int = None, origClientOrderId: str = None) -> Dict:
        """查询订单状态"""
        endpoint = "/api/v1/order"
        params = {'symbol': symbol}
        
        if order_id:
            params['orderId'] = order_id
        elif origClientOrderId:
            params['origClientOrderId'] = origClientOrderId
        else:
            return {'error': '必须提供orderId或origClientOrderId'}
            
        return self._request('GET', endpoint, params, signed=True)
    
    def get_account_balance(self, force_refresh: bool = False) -> Dict[str, AccountBalance]:
        """获取账户余额"""
        # 如果缓存存在且不强制刷新，直接返回缓存数据
        if self._balance_cache is not None and not force_refresh:
            return self._balance_cache
        
        endpoint = "/api/v1/account"
        data = self._request('GET', endpoint, signed=True)
        
        balances = {}
        if 'balances' in data:
            for balance in data['balances']:
                asset = balance['asset']
                balances[asset] = AccountBalance(
                    free=float(balance.get('free', 0)),
                    locked=float(balance.get('locked', 0))
                )
        
        # 更新缓存
        self._balance_cache = balances
        return balances
    
    def get_asset_balance(self, asset: str, force_refresh: bool = False) -> float:
        """获取指定资产的可用余额"""
        balances = self.get_account_balance(force_refresh)
        if asset in balances:
            return balances[asset].free + balances[asset].locked
        return 0.0
    
    def refresh_balance_cache(self):
        """强制刷新余额缓存"""
        self._balance_cache = None
        return self.get_account_balance(force_refresh=True)
    
    def get_all_user_trades(self, symbol: str, start_time: int = None, end_time: int = None) -> List[Dict]:
        """获取所有账户成交历史（分页获取所有记录）"""
        all_trades = []
        limit = 1000  # 每次获取的最大记录数
        from_id = 1  # 从最小的ID开始获取
        max_attempts = 1000  # 最大尝试次数，防止无限循环
        attempt_count = 0
        
        self.logger.info(f"开始获取 {symbol} 的所有成交历史，从ID=1开始...")
        
        while attempt_count < max_attempts:
            attempt_count += 1
            try:
                params = {
                    'symbol': symbol,
                    'limit': limit,
                    'fromId': from_id
                }
                
                if start_time:
                    params['startTime'] = start_time
                if end_time:
                    params['endTime'] = end_time
                
                self.logger.info(f"获取成交历史: fromId={from_id}, limit={limit}, 第{attempt_count}次尝试")
                
                endpoint = "/api/v1/userTrades"
                data = self._request('GET', endpoint, params, signed=True)
                
                if not isinstance(data, list):
                    self.logger.error(f"获取成交历史失败: {data}")
                    break
                
                if not data:
                    self.logger.info("没有更多成交记录了")
                    break
                
                # 过滤指定交易对的记录
                filtered_trades = [trade for trade in data if trade.get('symbol') == symbol]
                
                if not filtered_trades:
                    self.logger.info("没有找到指定交易对的成交记录")
                    break
                
                all_trades.extend(filtered_trades)
                
                self.logger.info(f"本次获取 {len(filtered_trades)} 条记录，累计 {len(all_trades)} 条记录")
                
                # 如果返回的记录数少于limit，说明已经获取完所有记录
                if len(data) < limit:
                    self.logger.info("已获取所有成交记录")
                    break
                
                # 设置下一次查询的起始ID（使用最大的trade ID + 1）
                max_trade_id = max(int(trade['id']) for trade in filtered_trades)
                from_id = max_trade_id + 1  # 获取更大的ID的记录
                
                # 避免频繁请求
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"获取成交历史时出错: {e}")
                break
        
        if attempt_count >= max_attempts:
            self.logger.warning(f"达到最大尝试次数 {max_attempts}，停止获取")
        
        self.logger.info(f"总共获取到 {len(all_trades)} 条 {symbol} 的成交记录")
        return all_trades
    
    def get_user_trades(self, symbol: str, start_time: int = None, end_time: int = None, 
                       limit: int = 1000, from_id: int = None) -> List[Dict]:
        """获取账户成交历史（兼容旧接口）"""
        # 如果指定了limit，使用原来的逻辑
        if limit and limit <= 1000:
            params = {
                'symbol': symbol,
                'limit': limit
            }
            
            if start_time:
                params['startTime'] = start_time
            if end_time:
                params['endTime'] = end_time
            if from_id:
                params['fromId'] = from_id
                
            data = self._request('GET', "/api/v1/userTrades", params, signed=True)
            
            if isinstance(data, list):
                return [trade for trade in data if trade.get('symbol') == symbol]
            else:
                self.logger.error(f"获取成交历史失败: {data}")
                return []
        else:
            # 如果需要获取所有记录，使用新的分页方法
            return self.get_all_user_trades(symbol, start_time, end_time)

class SmartMarketMaker:
    def __init__(self):
        self.symbol = os.getenv('SYMBOL', 'ATUSDT')
        self.base_asset = self.symbol.replace('USDT', '')
        self.quote_asset = 'USDT'
        self.quote_asset = 'ASTER'
        
        self.max_spread = float(os.getenv('MAX_SPREAD', 0.002))
        self.max_price_change = float(os.getenv('MAX_PRICE_CHANGE', 0.005))
        self.min_depth_multiplier = float(os.getenv('MIN_DEPTH_MULTIPLIER', 2))
        self.fixed_buy_quantity = float(os.getenv('TRADE_QUANTITY', 10))  # 固定买单数量
        self.target_volume = float(os.getenv('TARGET_VOLUME', 1000))
        self.check_interval = float(os.getenv('CHECK_INTERVAL', 1))
        self.max_retry = int(os.getenv('MAX_RETRY', 3))
        self.order_timeout = float(os.getenv('ORDER_TIMEOUT', 10))
        
        # 策略选择
        strategy_str = os.getenv('TRADING_STRATEGY', 'BOTH').upper()
        self.strategy = getattr(TradingStrategy, strategy_str, TradingStrategy.BOTH)
        
        # 初始化客户端
        self.client1 = AsterDexClient(
            os.getenv('ACCOUNT1_API_KEY'),
            os.getenv('ACCOUNT1_SECRET_KEY'),
            'ACCOUNT1'
        )
        self.client2 = AsterDexClient(
            os.getenv('ACCOUNT2_API_KEY'), 
            os.getenv('ACCOUNT2_SECRET_KEY'),
            'ACCOUNT2'
        )
        
        # 设置日志
        self.logger = logging.getLogger(__name__)
        
        # 预加载交易对精度信息
        self.preload_precision_info()
        
        # 缓存数据 - 初始化为None，表示需要首次计算
        self.cached_trade_direction = None
        
        # 交易状态
        self.total_volume = 0
        self.is_running = False
        self.order_book = OrderBook(bids=[], asks=[], update_time=0)
        self.last_prices = []
        self.price_history_size = 10
        
        # 交易统计
        self.trade_count = 0
        self.successful_trades = 0
        self.limit_sell_success_count = 0  # 卖单限价单成功次数
        self.market_sell_success_count = 0  # 卖单市价单成功次数
        self.limit_sell_attempt_count = 0   # 卖单限价单尝试次数
        self.partial_limit_sell_count = 0   # 卖单限价单部分成交次数
        
        # 历史交易量统计
        self.historical_volume_account1 = 0.0
        self.historical_volume_account2 = 0.0
        self.total_historical_volume = 0.0
        self.historical_trade_count_account1 = 0
        self.historical_trade_count_account2 = 0
        
    def calculate_historical_volume(self):
        """计算历史所有AT现货交易量总和（以USDT为单位）"""
        self.logger.info("📊 正在计算历史AT现货交易量...")
        
        # 计算账户1的历史交易量
        try:
            self.logger.info("获取账户1的所有成交历史...")
            trades_account1 = self.client1.get_all_user_trades(symbol=self.symbol)
            
            for trade in trades_account1:
                if trade.get('symbol') == self.symbol:
                    quote_qty = float(trade.get('quoteQty', 0))
                    self.historical_volume_account1 += quote_qty
                    self.historical_trade_count_account1 += 1
                    
            self.logger.info(f"✅ 账户1 {self.symbol} 历史交易: {self.historical_trade_count_account1} 笔, 交易量: {self.historical_volume_account1:.2f} USDT")
            
        except Exception as e:
            self.logger.error(f"❌ 获取账户1历史交易量失败: {e}")
        
        # 计算账户2的历史交易量
        try:
            self.logger.info("获取账户2的所有成交历史...")
            trades_account2 = self.client2.get_all_user_trades(symbol=self.symbol)
            
            for trade in trades_account2:
                if trade.get('symbol') == self.symbol:
                    quote_qty = float(trade.get('quoteQty', 0))
                    self.historical_volume_account2 += quote_qty
                    self.historical_trade_count_account2 += 1
                    
            self.logger.info(f"✅ 账户2 {self.symbol} 历史交易: {self.historical_trade_count_account2} 笔, 交易量: {self.historical_volume_account2:.2f} USDT")
            
        except Exception as e:
            self.logger.error(f"❌ 获取账户2历史交易量失败: {e}")
        
        self.total_historical_volume = self.historical_volume_account1 + self.historical_volume_account2
        total_trade_count = self.historical_trade_count_account1 + self.historical_trade_count_account2
        self.logger.info(f"💰 总历史AT现货交易: {total_trade_count} 笔, 交易量: {self.total_historical_volume:.2f} USDT")
        
        return self.total_historical_volume
    
    def initialize_at_balance(self) -> bool:
        """初始化AT余额：如果两个账号都没有AT，让其中一个账号市价买入"""
        at_balance1 = self.client1.get_asset_balance(self.base_asset)
        at_balance2 = self.client2.get_asset_balance(self.base_asset)
        
        self.logger.info(f"检查AT余额: 账户1={at_balance1:.4f}, 账户2={at_balance2:.4f}")
        
        # 如果两个账号都有AT或者都有USDT不足，不需要初始化
        if at_balance1 > 1 and at_balance2 > 1:
            self.logger.info("✅ 两个账户都有AT余额，无需初始化")
            return True
        
        # 如果两个账号都没有AT，选择一个账号买入
        if at_balance1 <= 1 and at_balance2 <= 1:
            self.logger.info("🔄 两个账户都没有AT余额，开始初始化...")
            
            # 选择USDT余额较多的账号进行买入
            usdt_balance1 = self.client1.get_asset_balance(self.quote_asset)
            usdt_balance2 = self.client2.get_asset_balance(self.quote_asset)
            
            if usdt_balance1 >= usdt_balance2 and usdt_balance1 > 0:
                # 账户1买入
                buy_client = self.client1
                buy_client_name = 'ACCOUNT1'
                available_usdt = usdt_balance1
            elif usdt_balance2 > 0:
                # 账户2买入
                buy_client = self.client2
                buy_client_name = 'ACCOUNT2'
                available_usdt = usdt_balance2
            else:
                self.logger.error("❌ 两个账户都没有足够的USDT进行初始化买入")
                return False
            
            # 计算可买入的AT数量（使用可用USDT的一半，避免全部用完）
            bid, ask, _, _ = self.get_best_bid_ask()
            if bid == 0 or ask == 0:
                self.logger.error("❌ 无法获取市场价格，初始化失败")
                return False
            
            current_price = (bid + ask) / 2
            buy_quantity = min(self.fixed_buy_quantity, (available_usdt * 0.5) / current_price)
            
            if buy_quantity <= 0:
                self.logger.error("❌ 计算出的买入数量为0，初始化失败")
                return False
            
            self.logger.info(f"🎯 选择 {buy_client_name} 进行初始化买入: 数量={buy_quantity:.4f}, 价格≈{current_price:.4f}")
            
            # 执行市价买入
            timestamp = int(time.time() * 1000)
            buy_order_id = f"{buy_client_name.lower()}_init_buy_{timestamp}"
            
            buy_order = buy_client.create_order(
                symbol=self.symbol,
                side='BUY',
                order_type='MARKET',
                quantity=buy_quantity,
                newClientOrderId=buy_order_id
            )
            
            if 'orderId' not in buy_order:
                self.logger.error(f"❌ 初始化买入失败: {buy_order}")
                return False
            
            self.logger.info(f"✅ 初始化买入订单已提交: {buy_order_id}")
            
            # 等待订单成交
            success = self.wait_for_orders_completion([(buy_client, buy_order_id)])
            
            if success:
                self.logger.info("✅ AT余额初始化成功")
                # 刷新余额缓存
                self.client1.refresh_balance_cache()
                self.client2.refresh_balance_cache()
                return True
            else:
                self.logger.error("❌ 初始化买入订单未成交")
                return False
        
        self.logger.info("✅ AT余额状态正常，无需初始化")
        return True
    
    def get_cached_trade_direction(self) -> Tuple[str, str]:
        """获取缓存的交易方向，如果缓存不存在则计算"""
        if self.cached_trade_direction is None:
            self.cached_trade_direction = self.determine_trade_direction()
        
        return self.cached_trade_direction
    
    def update_trade_direction_cache(self):
        """强制更新交易方向缓存"""
        self.cached_trade_direction = self.determine_trade_direction()
    
    def determine_trade_direction(self) -> Tuple[str, str]:
        """自动判断交易方向：返回 (sell_client_name, buy_client_name)"""
        # 使用缓存的余额数据
        at_balance1 = self.client1.get_asset_balance(self.base_asset)
        at_balance2 = self.client2.get_asset_balance(self.base_asset)
        
        self.logger.info(f"账户余额对比: 账户1 {self.base_asset}={at_balance1:.4f}, 账户2 {self.base_asset}={at_balance2:.4f}")
        
        if at_balance1 >= at_balance2:
            self.logger.info("🎯 选择策略: 账户1卖出，账户2买入")
            return 'ACCOUNT1', 'ACCOUNT2'
        else:
            self.logger.info("🎯 选择策略: 账户2卖出，账户1买入")
            return 'ACCOUNT2', 'ACCOUNT1'
    
    def get_current_trade_direction(self) -> Tuple[str, str]:
        """获取当前交易方向（使用缓存）"""
        return self.get_cached_trade_direction()
    
    def preload_precision_info(self):
        """预加载所有需要的交易对精度信息"""
        self.logger.info("🔄 预加载交易对精度信息...")
        
        success1 = self.client1.preload_symbol_precision(self.symbol)
        success2 = self.client2.preload_symbol_precision(self.symbol)
        
        if success1 and success2:
            self.logger.info("✅ 交易对精度信息预加载完成")
        else:
            self.logger.warning("⚠️ 交易对精度信息预加载部分失败，将使用默认精度")
        
        # 显示加载的精度信息
        tick_size1, step_size1 = self.client1.get_symbol_precision(self.symbol)
        tick_size2, step_size2 = self.client2.get_symbol_precision(self.symbol)
        
        self.logger.info(f"📊 账户1 {self.symbol}: 价格精度={tick_size1}, 数量精度={step_size1}")
        self.logger.info(f"📊 账户2 {self.symbol}: 价格精度={tick_size2}, 数量精度={step_size2}")
    
    def update_order_book(self):
        """更新订单簿数据"""
        try:
            new_order_book = self.client1.get_order_book(self.symbol, limit=10)
            if new_order_book.bids and new_order_book.asks:
                self.order_book = new_order_book
                
                # 更新价格历史
                mid_price = (new_order_book.bids[0][0] + new_order_book.asks[0][0]) / 2
                self.last_prices.append(mid_price)
                if len(self.last_prices) > self.price_history_size:
                    self.last_prices.pop(0)
                    
        except Exception as e:
            self.logger.error(f"更新订单簿时出错: {e}")
    
    def get_best_bid_ask(self) -> Tuple[float, float, float, float]:
        """获取最优买卖价和深度"""
        if not self.order_book.bids or not self.order_book.asks:
            return 0, 0, 0, 0
            
        best_bid = self.order_book.bids[0][0]
        best_ask = self.order_book.asks[0][0]
        bid_quantity = self.order_book.bids[0][1]
        ask_quantity = self.order_book.asks[0][1]
        
        return best_bid, best_ask, bid_quantity, ask_quantity
    
    def calculate_spread_percentage(self, bid: float, ask: float) -> float:
        """计算价差百分比"""
        if bid == 0 or ask == 0:
            return float('inf')
        return (ask - bid) / bid
    
    def calculate_price_volatility(self) -> float:
        """计算价格波动率"""
        if len(self.last_prices) < 2:
            return 0
            
        returns = []
        for i in range(1, len(self.last_prices)):
            if self.last_prices[i-1] != 0:
                returns.append(abs(self.last_prices[i] - self.last_prices[i-1]) / self.last_prices[i-1])
        
        return max(returns) if returns else 0
    
    def get_sell_quantity(self, sell_client_name: str = None) -> Tuple[float, str]:
        """获取实际可卖数量和卖出账户（使用缓存余额）"""
        if sell_client_name is None:
            sell_client_name, _ = self.get_current_trade_direction()
        
        if sell_client_name == 'ACCOUNT1':
            available_at = self.client1.get_asset_balance(self.base_asset)
            sell_account = 'ACCOUNT1'
        else:
            available_at = self.client2.get_asset_balance(self.base_asset)
            sell_account = 'ACCOUNT2'
        
        return available_at, sell_account

    def check_buy_conditions_with_retry(self, max_retry: int = 3, wait_time: int = 20) -> bool:
        """检查买单条件，余额不足时等待并重试"""
        for attempt in range(max_retry):
            if self.check_buy_conditions():
                return True
            else:
                if attempt < max_retry - 1:  # 不是最后一次尝试
                    self.logger.info(f"USDT余额不足，等待{wait_time}秒后重试... (尝试 {attempt + 1}/{max_retry})")
                    
                    # 强制刷新余额缓存
                    self.client1.refresh_balance_cache()
                    self.client2.refresh_balance_cache()
                    self.update_trade_direction_cache()
                    
                    time.sleep(wait_time)
        
        return False

    def check_sell_conditions_with_retry(self, max_retry: int = 3, wait_time: int = 20) -> bool:
        """检查卖单条件，余额不足时等待并重试"""
        for attempt in range(max_retry):
            if self.check_sell_conditions():
                return True
            else:
                if attempt < max_retry - 1:  # 不是最后一次尝试
                    self.logger.info(f"AT余额不足，等待{wait_time}秒后重试... (尝试 {attempt + 1}/{max_retry})")
                    
                    # 强制刷新余额缓存
                    self.client1.refresh_balance_cache()
                    self.client2.refresh_balance_cache()
                    self.update_trade_direction_cache()
                    
                    time.sleep(wait_time)
        
        return False
    
    def check_buy_conditions(self) -> bool:
        """检查买单条件：USDT余额是否足够（使用缓存余额）"""
        _, buy_client_name = self.get_current_trade_direction()
        
        if buy_client_name == 'ACCOUNT1':
            # 账户1买AT，需要USDT
            available_usdt = self.client1.get_asset_balance(self.quote_asset)
        else:
            # 账户2买AT，需要USDT
            available_usdt = self.client2.get_asset_balance(self.quote_asset)
        
        # 计算需要的USDT金额
        bid, ask, _, _ = self.get_best_bid_ask()
        if bid == 0 or ask == 0:
            return False
        
        current_price = (bid + ask) / 2
        required_usdt = self.fixed_buy_quantity * current_price
        
        if available_usdt >= required_usdt:
            return True
        else:
            self.logger.warning(f"USDT余额不足: 需要{required_usdt:.2f}, 当前{available_usdt:.2f}")
            return False
    
    def check_sell_conditions(self) -> bool:
        """检查卖单条件：AT余额是否足够（至少要有一些AT可卖）"""
        sell_quantity, sell_account = self.get_sell_quantity()
        if sell_quantity <= 0:
            self.logger.warning(f"账户 {sell_account} 无可卖{self.base_asset}数量")
            return False
        return True
    
    def check_market_conditions(self) -> bool:
        """检查市场条件是否满足交易（包含余额不足重试机制）"""
        """检查市场条件是否满足交易（包含余额不足重试机制）"""
        # 检查AT余额状态，如果两个账号都没有AT，先初始化
        at_balance1 = self.client1.get_asset_balance(self.base_asset)
        at_balance2 = self.client2.get_asset_balance(self.base_asset)
        
        if at_balance1 <= 0 and at_balance2 <= 0:
            self.logger.warning("⚠️ 两个账户都没有AT余额，尝试初始化...")
            if self.initialize_at_balance():
                self.logger.info("✅ AT余额初始化成功，继续交易")
            else:
                self.logger.error("❌ AT余额初始化失败，暂停交易")
                return False
            
        # 检查卖单条件（使用重试机制）
        if not self.check_sell_conditions_with_retry(max_retry=3, wait_time=20):
            self.logger.error("卖单条件检查失败，AT余额持续不足")
            return False
        
        # 检查买单条件（使用重试机制）
        if not self.check_buy_conditions_with_retry(max_retry=3, wait_time=20):
            self.logger.error("买单条件检查失败，USDT余额持续不足")
            return False
        
        # 原有的市场条件检查
        bid, ask, bid_qty, ask_qty = self.get_best_bid_ask()
        
        if bid == 0 or ask == 0:
            return False
            
        # 检查价差
        spread = self.calculate_spread_percentage(bid, ask)
        if spread > self.max_spread:
            self.logger.warning(f"价差过大: {spread:.4%} > {self.max_spread:.4%}")
            return False
        
        # 检查价格波动
        volatility = self.calculate_price_volatility()
        if volatility > self.max_price_change:
            self.logger.warning(f"价格波动过大: {volatility:.4%} > {self.max_price_change:.4%}")
            return False
        
        # 检查深度
        min_required_depth = self.fixed_buy_quantity * self.min_depth_multiplier
        if bid_qty < min_required_depth or ask_qty < min_required_depth:
            self.logger.warning(f"深度不足: 买一量={bid_qty:.2f}, 卖一量={ask_qty:.2f}, 要求={min_required_depth:.2f}")
            return False
            
        sell_quantity, sell_account = self.get_sell_quantity()
        _, buy_account = self.get_current_trade_direction()
        
        self.logger.info(f"✓ 市场条件满足: 价差={spread:.4%}, 波动={volatility:.4%}")
        self.logger.info(f"  交易方向: {sell_account}卖出{sell_quantity:.4f}, {buy_account}买入{self.fixed_buy_quantity:.4f}")
        return True
    
    def strategy_market_only(self) -> bool:
        """策略1: 同时挂市价单对冲"""
        self.logger.info("执行策略1: 同时市价单对冲")
        
        try:
            timestamp = int(time.time() * 1000)
            
            # 动态获取交易方向（使用缓存）
            sell_client_name, buy_client_name = self.get_current_trade_direction()
            
            # 确定买卖客户端
            sell_client = self.client1 if sell_client_name == 'ACCOUNT1' else self.client2
            buy_client = self.client1 if buy_client_name == 'ACCOUNT1' else self.client2
            
            # 生成订单ID
            sell_order_id = f"{sell_client_name.lower()}_sell_{timestamp}"
            buy_order_id = f"{buy_client_name.lower()}_buy_{timestamp}"
            
            # 卖单数量：实际持有量
            sell_quantity, _ = self.get_sell_quantity(sell_client_name)
            # 买单数量：固定配置量
            buy_quantity = self.fixed_buy_quantity
            
            self.logger.info(f"交易详情: {sell_client_name}卖出={sell_quantity:.4f}, {buy_client_name}买入={buy_quantity:.4f}")
            
            # 同时下市价单
            sell_order = sell_client.create_order(
                symbol=self.symbol,
                side='SELL',
                order_type='MARKET',
                quantity=sell_quantity,
                newClientOrderId=sell_order_id
            )
            
            if 'orderId' not in sell_order:
                self.logger.error(f"市价卖单失败: {sell_order}")
                return False
            
            buy_order = buy_client.create_order(
                symbol=self.symbol,
                side='BUY',
                order_type='MARKET',
                quantity=buy_quantity,
                newClientOrderId=buy_order_id
            )
            
            if 'orderId' not in buy_order:
                self.logger.error(f"市价买单失败: {buy_order}")
                sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
                return False
            
            self.logger.info(f"市价单对冲已提交: 卖单={sell_order_id}, 买单={buy_order_id}")
            
            # 等待并检查成交
            success = self.wait_for_orders_completion([
                (sell_client, sell_order_id),
                (buy_client, buy_order_id)
            ])
            
            # 交易成功后更新缓存和统计
            if success:
                self.market_sell_success_count += 1
                self.update_cache_after_trade()
            
            return success
            
        except Exception as e:
            self.logger.error(f"策略1执行出错: {e}")
            return False
    
    def handle_partial_limit_sell(self, sell_client:AsterDexClient, sell_order_id, sell_client_name, timestamp) -> bool:
        """处理限价卖单部分成交的情况"""
        self.logger.info("🔄 检测到限价卖单部分成交，处理剩余数量...")
        
        try:
            # 首先取消剩余的限价单
            cancel_result = sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
            if 'orderId' in cancel_result:
                self.logger.info("✅ 已取消剩余限价卖单")
            else:
                self.logger.warning("⚠️ 取消限价卖单失败，但继续执行市价卖出")
            
            # 强制刷新余额缓存，获取最新余额（包括已成交部分）
            sell_client.refresh_balance_cache()
            
            # 获取当前实际剩余可卖数量
            if sell_client_name == 'ACCOUNT1':
                remaining_quantity = self.client1.get_asset_balance(self.base_asset)
            else:
                remaining_quantity = self.client2.get_asset_balance(self.base_asset)
            self.logger.info(f"📤 限价卖单部分成交 剩余 {remaining_quantity:.4f} {self.base_asset} ")

            if remaining_quantity > 0:
                self.logger.info(f"📤 剩余 {remaining_quantity:.4f} {self.base_asset} 需要市价卖出")
                
                # 立即下市价卖单，卖出剩余的全部AT数量
                emergency_sell = sell_client.create_order(
                    symbol=self.symbol,
                    side='SELL',
                    order_type='MARKET',
                    quantity=remaining_quantity,
                    newClientOrderId=f"emergency_sell_{timestamp}"
                )
                
                if 'orderId' in emergency_sell:
                    self.logger.info(f"✅ 紧急市价卖单已提交: 数量={remaining_quantity:.4f}")
                    
                    # 等待卖单成交
                    time.sleep(2)
                    
                    # 检查卖单状态
                    sell_status = sell_client.get_order(self.symbol, origClientOrderId=f"emergency_sell_{timestamp}")
                    if sell_status.get('status') in ['FILLED', 'PARTIALLY_FILLED']:
                        self.logger.info("✅ 紧急市价卖单已成交")
                        # 强制刷新余额缓存，确保数据最新
                        sell_client.refresh_balance_cache()
                        self.market_sell_success_count += 1
                        self.partial_limit_sell_count += 1
                        return True
                    else:
                        self.logger.warning("⚠️ 紧急市价卖单未完全成交")
                        return False
                else:
                    self.logger.error("❌ 紧急市价卖单失败")
                    return False
            else:
                self.logger.info("✅ 限价卖单已完全成交，无需额外操作")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 处理部分成交时出错: {e}")
            return False
    
    def strategy_limit_market(self) -> bool:
        """策略2: 限价卖单 + 市价买单"""
        self.logger.info("执行策略2: 限价卖单 + 市价买单")
        
        try:
            bid, ask, _, _ = self.get_best_bid_ask()
            timestamp = int(time.time() * 1000)
            
            # 动态获取交易方向（使用缓存）
            sell_client_name, buy_client_name = self.get_current_trade_direction()
            
            # 确定买卖客户端
            sell_client = self.client1 if sell_client_name == 'ACCOUNT1' else self.client2
            buy_client = self.client1 if buy_client_name == 'ACCOUNT1' else self.client2
            
            # 生成订单ID
            sell_order_id = f"{sell_client_name.lower()}_limit_sell_{timestamp}"
            buy_order_id = f"{buy_client_name.lower()}_market_buy_{timestamp}"
            
            # 卖单数量：实际持有量
            sell_quantity, _ = self.get_sell_quantity(sell_client_name)
            # 买单数量：固定配置量
            buy_quantity = self.fixed_buy_quantity
            
            # 设置卖单价格为卖一价减0.00001
            sell_price = ask - 0.00001
            
            self.logger.info(f"交易详情: {sell_client_name}卖出={sell_quantity:.4f}@{sell_price:.5f}, {buy_client_name}买入={buy_quantity:.4f}")
            
            # 记录限价卖单尝试
            self.limit_sell_attempt_count += 1
            
            # 挂限价卖单（实际持有量）
            sell_order = sell_client.create_order(
                symbol=self.symbol,
                side='SELL',
                order_type='LIMIT',
                quantity=sell_quantity,
                price=sell_price,
                newClientOrderId=sell_order_id
            )
            
            if 'orderId' not in sell_order:
                self.logger.error(f"限价卖单失败: {sell_order}")
                return False
            
            self.logger.info(f"限价卖单已挂出: 价格={sell_price:.6f}, 数量={sell_quantity:.4f}, 订单ID={sell_order_id}")
            
            # 下市价买单（固定配置量）
            buy_order = buy_client.create_order(
                symbol=self.symbol,
                side='BUY',
                order_type='MARKET',
                quantity=buy_quantity,
                newClientOrderId=buy_order_id
            )
            
            if 'orderId' not in buy_order:
                self.logger.error(f"市价买单失败: {buy_order}")
                sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
                return False
            
            self.logger.info(f"市价买单已提交: 订单ID={buy_order_id}")
            
            # 监控订单状态
            start_time = time.time()
            buy_filled = False
            sell_filled = False
            sell_was_limit = True  # 标记卖单是否为限价单
            sell_partial_filled = False  # 标记卖单是否部分成交
            
            while time.time() - start_time < self.order_timeout:
                # 检查买单状态
                if not buy_filled:
                    buy_status = buy_client.get_order(self.symbol, origClientOrderId=buy_order_id)
                    if buy_status.get('status') in ['FILLED', 'PARTIALLY_FILLED']:
                        buy_filled = True
                        self.logger.info("市价买单已成交")
                
                # 检查卖单状态
                if not sell_filled:
                    sell_status = sell_client.get_order(self.symbol, origClientOrderId=sell_order_id)
                    sell_status_value = sell_status.get('status')
                    
                    if sell_status_value == 'FILLED':
                        sell_filled = True
                        self.logger.info("限价卖单已完全成交")
                        self.limit_sell_success_count += 1
                    
                    elif sell_status_value == 'PARTIALLY_FILLED':
                        self.logger.warning("⚠️ 限价卖单部分成交")
                        sell_partial_filled = True
                        
                        # 如果买单已成交但卖单部分成交，处理剩余数量
                        if buy_filled:
                            # 处理部分成交
                            success = self.handle_partial_limit_sell(sell_client, sell_order_id, sell_client_name, timestamp)
                            if success:
                                sell_filled = True
                                sell_was_limit = False  # 标记为已转为市价单
                            break
                
                if buy_filled and sell_filled:
                    break
                    
                # 如果买单成交但卖单未成交，转为市价卖出
                if buy_filled and not sell_filled and not sell_partial_filled:
                    self.logger.warning("检测到风险: 买单成交但卖单未成交，转为市价卖出")
                    sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
                    
                    # 标记卖单已转为市价单
                    sell_was_limit = False
                    
                    # 立即下市价卖单，卖出实际持有的AT数量
                    emergency_sell_quantity, _ = self.get_sell_quantity(sell_client_name)
                    if emergency_sell_quantity > 0:
                        emergency_sell = sell_client.create_order(
                            symbol=self.symbol,
                            side='SELL',
                            order_type='MARKET',
                            quantity=emergency_sell_quantity,
                            newClientOrderId=f"emergency_sell_{timestamp}"
                        )
                        
                        if 'orderId' in emergency_sell:
                            self.logger.info(f"紧急市价卖单已提交: 数量={emergency_sell_quantity:.4f}")
                            # 等待卖单成交
                            time.sleep(2)
                            sell_filled = True
                            # 记录市价卖单成功
                            self.market_sell_success_count += 1
                        else:
                            self.logger.error("紧急市价卖单失败")
                            return False
                    else:
                        self.logger.warning("无可卖AT数量，无法进行紧急卖出")
                        return False
                
                time.sleep(0.5)
            
            # 清理未成交订单
            if not buy_filled:
                buy_client.cancel_order(self.symbol, origClientOrderId=buy_order_id)
            if not sell_filled and sell_was_limit and not sell_partial_filled:
                sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
            
            success = buy_filled and sell_filled
            
            # 交易成功后更新缓存
            if success:
                self.update_cache_after_trade()
            
            return success
            
        except Exception as e:
            self.logger.error(f"策略2执行出错: {e}")
            return False
    
    def wait_for_orders_completion(self, orders: List[Tuple[AsterDexClient, str]]) -> bool:
        """等待订单完成"""
        start_time = time.time()
        completed = [False] * len(orders)
        
        while time.time() - start_time < self.order_timeout:
            all_completed = True
            
            for i, (client, order_id) in enumerate(orders):
                if not completed[i]:
                    order_status = client.get_order(self.symbol, origClientOrderId=order_id)
                    if order_status.get('status') in ['FILLED', 'PARTIALLY_FILLED']:
                        completed[i] = True
                        self.logger.info(f"订单 {order_id} 已成交")
                    elif order_status.get('status') in ['CANCELED', 'REJECTED', 'EXPIRED']:
                        self.logger.error(f"订单 {order_id} 失败: {order_status.get('status')}")
                        # 取消所有相关订单
                        for j, (other_client, other_id) in enumerate(orders):
                            if j != i and not completed[j]:
                                other_client.cancel_order(self.symbol, origClientOrderId=other_id)
                        return False
                    else:
                        all_completed = False
            
            if all_completed:
                return True
            
            time.sleep(0.5)
        
        # 超时，取消所有未完成订单
        self.logger.warning("订单等待超时，取消未完成订单")
        for client, order_id in orders:
            if not any(c[1] == order_id and completed[i] for i, c in enumerate(orders)):
                client.cancel_order(self.symbol, origClientOrderId=order_id)
        
        return False
    
    def update_cache_after_trade(self):
        """交易成功后更新缓存数据"""
        self.logger.info("🔄 交易成功，更新缓存数据...")
        
        # 强制刷新余额缓存
        self.client1.refresh_balance_cache()
        self.client2.refresh_balance_cache()
        
        # 更新交易方向缓存
        self.update_trade_direction_cache()
        
        self.logger.info("✅ 缓存数据已更新")
    
    def update_cache_after_failure(self):
        """交易失败后更新缓存数据"""
        self.logger.info("🔄 交易失败，更新缓存数据...")
        
        # 强制刷新余额缓存
        self.client1.refresh_balance_cache()
        self.client2.refresh_balance_cache()
        
        # 更新交易方向缓存
        self.update_trade_direction_cache()
        
        self.logger.info("✅ 缓存数据已更新")
    
    def execute_trading_cycle(self) -> bool:
        """执行一个交易周期"""
        if not self.check_market_conditions():
            return False
        
        self.trade_count += 1
        
        success = False
        
        if self.strategy in [TradingStrategy.MARKET_ONLY, TradingStrategy.BOTH]:
            success = self.strategy_market_only()
            if success:
                self.successful_trades += 1
        
        if self.strategy in [TradingStrategy.LIMIT_MARKET, TradingStrategy.BOTH] and not success:
            success = self.strategy_limit_market()
            if success:
                self.successful_trades += 1
        
        if success:
            # 交易量计算：买卖双方都计入，使用固定买单数量
            trade_volume = self.fixed_buy_quantity * 2
            self.total_volume += trade_volume
            
            # 显示当前交易方向
            sell_account, buy_account = self.get_current_trade_direction()
            self.logger.info(f"✓ 交易成功! {sell_account}卖出 → {buy_account}买入")
            self.logger.info(f"  本次交易量: {trade_volume:.4f}, 累计: {self.total_volume:.2f}/{self.target_volume}")
        else:
            self.logger.error("✗ 交易失败")
            # 交易失败后也更新缓存
            self.update_cache_after_failure()
        
        return success
    
    def print_trading_statistics(self):
        """打印交易统计信息"""
        self.logger.info("\n📊 交易统计信息:")
        self.logger.info(f"   总尝试次数: {self.trade_count}")
        self.logger.info(f"   成功交易次数: {self.successful_trades}")
        
        if self.trade_count > 0:
            success_rate = (self.successful_trades / self.trade_count) * 100
            self.logger.info(f"   总体成功率: {success_rate:.1f}%")
        
        self.logger.info(f"   卖单限价单尝试次数: {self.limit_sell_attempt_count}")
        self.logger.info(f"   卖单限价单成功次数: {self.limit_sell_success_count}")
        self.logger.info(f"   卖单限价单部分成交次数: {self.partial_limit_sell_count}")
        
        if self.limit_sell_attempt_count > 0:
            limit_sell_success_rate = (self.limit_sell_success_count / self.limit_sell_attempt_count) * 100
            self.logger.info(f"   卖单限价单成功率: {limit_sell_success_rate:.1f}%")
        
        self.logger.info(f"   卖单市价单成功次数: {self.market_sell_success_count}")
        self.logger.info(f"   累计交易量: {self.total_volume:.2f}/{self.target_volume}")
    
    def print_historical_volume_statistics(self):
        """打印历史交易量统计"""
        self.logger.info("\n💰 历史AT现货交易量统计:")
        self.logger.info(f"   账户1 {self.symbol} 历史交易: {self.historical_trade_count_account1} 笔, 交易量: {self.historical_volume_account1:.2f} USDT")
        self.logger.info(f"   账户2 {self.symbol} 历史交易: {self.historical_trade_count_account2} 笔, 交易量: {self.historical_volume_account2:.2f} USDT")
        total_trade_count = self.historical_trade_count_account1 + self.historical_trade_count_account2
        total_historical_volume = self.historical_volume_account1 + self.historical_volume_account2
        self.logger.info(f"   总历史AT现货交易: {total_trade_count} 笔, 交易量: {total_historical_volume:.2f} USDT")
    
    def print_account_balances(self):
        """打印账户余额（使用缓存数据）"""
        try:
            # 使用缓存数据获取余额
            at_balance1 = self.client1.get_asset_balance(self.base_asset)
            usdt_balance1 = self.client1.get_asset_balance(self.quote_asset)
            aster_balance1 = self.client1.get_asset_balance(self.aster_asset)
            
            at_balance2 = self.client2.get_asset_balance(self.base_asset)
            usdt_balance2 = self.client2.get_asset_balance(self.quote_asset)
            aster_balance2 = self.client2.get_asset_balance(self.aster_asset)
            
            self.logger.info(f"账户1: {self.base_asset}={at_balance1:.4f}, {self.quote_asset}={usdt_balance1:.2f}")
            self.logger.info(f"账户2: {self.base_asset}={at_balance2:.4f}, {self.quote_asset}={usdt_balance2:.2f}")
            
            # 显示当前推荐交易方向
            sell_account, buy_account = self.get_current_trade_direction()
            self.logger.info(f"推荐方向: {sell_account}卖出 → {buy_account}买入")
            
        except Exception as e:
            self.logger.error(f"获取余额时出错: {e}")
    
    def monitor_and_trade(self):
        """监控市场并执行交易"""
        self.logger.info("开始智能刷量交易...")
        self.is_running = True
        
        consecutive_failures = 0
        
        while self.is_running and self.total_volume < self.target_volume:
            try:
                # 更新市场数据
                self.update_order_book()
                
                # 执行交易
                if self.execute_trading_cycle():
                    consecutive_failures = 0
                    # 每5次成功交易打印一次余额和统计
                    if self.successful_trades % 5 == 0:
                        self.print_account_balances()
                        self.print_trading_statistics()
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        self.logger.warning("连续多次交易失败，暂停20秒...")
                        time.sleep(20)
                        consecutive_failures = 0
                
                # 显示进度
                progress = self.total_volume / self.target_volume * 100
                success_rate = (self.successful_trades / self.trade_count * 100) if self.trade_count > 0 else 0
                self.logger.info(f"进度: {progress:.1f}% ({self.total_volume:.2f}/{self.target_volume}), 成功率: {success_rate:.1f}%")
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                self.logger.error(f"交易周期出错: {e}")
                time.sleep(self.check_interval)
        
        if self.total_volume >= self.target_volume:
            self.logger.info(f"🎉 达到目标交易量: {self.total_volume:.2f}")
        else:
            self.logger.info("交易已停止")
    
    def start(self):
        """启动交易程序"""
        self.logger.info("=" * 60)
        self.logger.info("智能刷量交易程序启动 - 自动判断交易方向")
        self.logger.info(f"交易对: {self.symbol}")
        self.logger.info(f"基础资产: {self.base_asset}")
        self.logger.info(f"策略: {self.strategy.value}")
        self.logger.info(f"固定买单数量: {self.fixed_buy_quantity}")
        self.logger.info(f"目标交易量: {self.target_volume}")
        self.logger.info(f"价差阈值: {self.max_spread:.2%}")
        self.logger.info(f"波动阈值: {self.max_price_change:.2%}")
        self.logger.info("=" * 60)
        
        # 初始化缓存
        self.logger.info("🔄 初始化缓存数据...")
        self.client1.refresh_balance_cache()
        self.client2.refresh_balance_cache()
        self.update_trade_direction_cache()
        self.logger.info("✅ 缓存数据初始化完成")

        self.update_order_book()

        # 检查并初始化AT余额
        self.logger.info("\n🔍 检查AT余额状态...")
        if not self.initialize_at_balance():
            self.logger.error("❌ AT余额初始化失败，程序退出")
            return
        
        # 计算历史交易量
        self.logger.info("\n📊 开始统计历史AT现货交易量...")
        self.calculate_historical_volume()
        
        # 打印初始余额和推荐方向
        self.logger.info("\n初始账户余额和推荐交易方向:")
        self.print_account_balances()
        self.logger.info("")
        
        # 启动交易
        self.logger.info("\n5s后开始交易...")
        time.sleep(5)
        self.monitor_and_trade()
    
    def stop(self):
        """停止交易"""
        self.is_running = False
        self.logger.info("\n交易程序已停止")
        self.logger.info("=" * 50)
        self.logger.info("最终交易统计:")
        self.print_trading_statistics()
        self.logger.info("\n历史交易量统计:")
        self.print_historical_volume_statistics()
        self.logger.info("=" * 50)
        self.logger.info("最终账户余额:")
        self.print_account_balances()

def main():
    """主函数"""
    maker = SmartMarketMaker()
    
    try:
        maker.start()
    except KeyboardInterrupt:
        logger.info("\n收到停止信号...")
    except Exception as e:
        logger.error(f"程序运行出错: {e}")
    finally:
        maker.stop()

if __name__ == "__main__":
    main()