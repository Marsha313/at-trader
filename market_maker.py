import requests
import time
import hmac
import hashlib
import urllib.parse
from typing import Dict, List, Optional, Tuple
import json
import threading
from dataclasses import dataclass
import os
from dotenv import load_dotenv
from enum import Enum

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
            print(f"API请求错误 ({self.account_name}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f" 错误响应: {e.response.text}")
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
                
                print(f"📊 {symbol} 步长信息: 价格={default_tick_size}, 数量={default_step_size}")
                self.symbol_precision_cache[symbol] = (default_tick_size, default_step_size)
                return True
            else:
                print(f"⚠️ 无法获取 {symbol} 的交易对信息，使用默认步长")
                self.symbol_precision_cache[symbol] = (default_tick_size, default_step_size)
                return False
        
        except Exception as e:
            print(f"获取交易对信息失败: {e}, 使用默认步长")
            self.symbol_precision_cache[symbol] = (default_tick_size, default_step_size)
            return False
    
    def get_symbol_precision(self, symbol: str) -> Tuple[float, float]:
        """获取交易对的步长信息（从缓存中）"""
        return (0.00001, 0.00001)
    
    def __get_trimmed_quantity(self, quantity: float, step_size: float) -> float:
        """格式化数量到正确的步长"""
        if step_size <= 0:
            return quantity
            
        trimmed_quantity = round(quantity / step_size) * step_size
        return trimmed_quantity
    
    def __get_trimmed_price(self, price: float, tick_size: float) -> float:
        """格式化价格到正确的步长"""
        if tick_size <= 0:
            return price
            
        trimmed_price = round(price / tick_size) * tick_size
        return trimmed_price
    
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
        
        # 从缓存获取步长信息
        tick_size, step_size = self.get_symbol_precision(symbol)
        
        # 格式化数量
        formatted_quantity = round(quantity,2)
        
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
        
        print(f"📤 发送订单请求:")
        print(f"   交易对: {symbol}")
        print(f"   方向: {side}")
        print(f"   类型: {order_type}")
        print(f"   数量: {quantity} -> {formatted_quantity}")
        if formatted_price:
            print(f"   价格: {price} -> {formatted_price}")
        
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
            return balances[asset].free
        return 0.0
    
    def refresh_balance_cache(self):
        """强制刷新余额缓存"""
        self._balance_cache = None
        return self.get_account_balance(force_refresh=True)

class SmartMarketMaker:
    def __init__(self):
        self.symbol = os.getenv('SYMBOL', 'ATUSDT')
        self.base_asset = self.symbol.replace('USDT', '')
        self.quote_asset = 'USDT'
        
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
        
        print(f"账户余额对比: 账户1 {self.base_asset}={at_balance1:.4f}, 账户2 {self.base_asset}={at_balance2:.4f}")
        
        if at_balance1 >= at_balance2:
            print("🎯 选择策略: 账户1卖出，账户2买入")
            return 'ACCOUNT1', 'ACCOUNT2'
        else:
            print("🎯 选择策略: 账户2卖出，账户1买入")
            return 'ACCOUNT2', 'ACCOUNT1'
    
    def get_current_trade_direction(self) -> Tuple[str, str]:
        """获取当前交易方向（使用缓存）"""
        return self.get_cached_trade_direction()
    
    def preload_precision_info(self):
        """预加载所有需要的交易对精度信息"""
        print("🔄 预加载交易对精度信息...")
        
        success1 = self.client1.preload_symbol_precision(self.symbol)
        success2 = self.client2.preload_symbol_precision(self.symbol)
        
        if success1 and success2:
            print("✅ 交易对精度信息预加载完成")
        else:
            print("⚠️ 交易对精度信息预加载部分失败，将使用默认精度")
        
        # 显示加载的精度信息
        tick_size1, step_size1 = self.client1.get_symbol_precision(self.symbol)
        tick_size2, step_size2 = self.client2.get_symbol_precision(self.symbol)
        
        print(f"📊 账户1 {self.symbol}: 价格精度={tick_size1}, 数量精度={step_size1}")
        print(f"📊 账户2 {self.symbol}: 价格精度={tick_size2}, 数量精度={step_size2}")
    
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
            print(f"更新订单簿时出错: {e}")
    
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
    
    def get_sell_quantity(self) -> Tuple[float, str]:
        """获取实际可卖数量和卖出账户（使用缓存余额）"""
        sell_client_name, _ = self.get_current_trade_direction()
        
        if sell_client_name == 'ACCOUNT1':
            available_at = self.client1.get_asset_balance(self.base_asset)
            sell_account = 'ACCOUNT1'
        else:
            available_at = self.client2.get_asset_balance(self.base_asset)
            sell_account = 'ACCOUNT2'
        
        return available_at, sell_account
    
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
            print(f"USDT余额不足: 需要{required_usdt:.2f}, 当前{available_usdt:.2f}")
            return False
    
    def check_sell_conditions(self) -> bool:
        """检查卖单条件：AT余额是否足够（至少要有一些AT可卖）"""
        sell_quantity, sell_account = self.get_sell_quantity()
        if sell_quantity <= 0:
            print(f"账户 {sell_account} 无可卖{self.base_asset}数量")
            return False
        return True
    
    def check_market_conditions(self) -> bool:
        """检查市场条件是否满足交易"""
        # 检查卖单条件（只要有AT可卖就行）
        if not self.check_sell_conditions():
            return False
        
        # 检查买单条件
        if not self.check_buy_conditions():
            return False
        
        bid, ask, bid_qty, ask_qty = self.get_best_bid_ask()
        
        if bid == 0 or ask == 0:
            return False
            
        # 检查价差
        spread = self.calculate_spread_percentage(bid, ask)
        if spread > self.max_spread:
            print(f"价差过大: {spread:.4%} > {self.max_spread:.4%}")
            return False
        
        # 检查价格波动
        volatility = self.calculate_price_volatility()
        if volatility > self.max_price_change:
            print(f"价格波动过大: {volatility:.4%} > {self.max_price_change:.4%}")
            return False
        
        # 检查深度
        min_required_depth = self.fixed_buy_quantity * self.min_depth_multiplier
        if bid_qty < min_required_depth or ask_qty < min_required_depth:
            print(f"深度不足: 买一量={bid_qty:.2f}, 卖一量={ask_qty:.2f}, 要求={min_required_depth:.2f}")
            return False
            
        sell_quantity, sell_account = self.get_sell_quantity()
        _, buy_account = self.get_current_trade_direction()
        
        print(f"✓ 市场条件满足: 价差={spread:.4%}, 波动={volatility:.4%}")
        print(f"  交易方向: {sell_account}卖出{sell_quantity:.4f}, {buy_account}买入{self.fixed_buy_quantity:.4f}")
        return True
    
    def strategy_market_only(self) -> bool:
        """策略1: 同时挂市价单对冲"""
        print("执行策略1: 同时市价单对冲")
        
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
            sell_quantity, _ = self.get_sell_quantity()
            # 买单数量：固定配置量
            buy_quantity = self.fixed_buy_quantity
            
            print(f"交易详情: {sell_client_name}卖出={sell_quantity:.4f}, {buy_client_name}买入={buy_quantity:.4f}")
            
            # 同时下市价单
            sell_order = sell_client.create_order(
                symbol=self.symbol,
                side='SELL',
                order_type='MARKET',
                quantity=sell_quantity,
                newClientOrderId=sell_order_id
            )
            
            if 'orderId' not in sell_order:
                print(f"市价卖单失败: {sell_order}")
                return False
            
            buy_order = buy_client.create_order(
                symbol=self.symbol,
                side='BUY',
                order_type='MARKET',
                quantity=buy_quantity,
                newClientOrderId=buy_order_id
            )
            
            if 'orderId' not in buy_order:
                print(f"市价买单失败: {buy_order}")
                sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
                return False
            
            print(f"市价单对冲已提交: 卖单={sell_order_id}, 买单={buy_order_id}")
            
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
            print(f"策略1执行出错: {e}")
            return False
    
    def strategy_limit_market(self) -> bool:
        """策略2: 限价卖单 + 市价买单"""
        print("执行策略2: 限价卖单 + 市价买单")
        
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
            sell_quantity, _ = self.get_sell_quantity()
            # 买单数量：固定配置量
            buy_quantity = self.fixed_buy_quantity
            
            # 设置卖单价格为卖一价减0.00001
            sell_price = ask - 0.00001
            
            print(f"交易详情: {sell_client_name}卖出={sell_quantity:.4f}@{sell_price:.5f}, {buy_client_name}买入={buy_quantity:.4f}")
            
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
                print(f"限价卖单失败: {sell_order}")
                return False
            
            print(f"限价卖单已挂出: 价格={sell_price:.6f}, 数量={sell_quantity:.4f}, 订单ID={sell_order_id}")
            
            # 下市价买单（固定配置量）
            buy_order = buy_client.create_order(
                symbol=self.symbol,
                side='BUY',
                order_type='MARKET',
                quantity=buy_quantity,
                newClientOrderId=buy_order_id
            )
            
            if 'orderId' not in buy_order:
                print(f"市价买单失败: {buy_order}")
                sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
                return False
            
            print(f"市价买单已提交: 订单ID={buy_order_id}")
            
            # 监控订单状态
            start_time = time.time()
            buy_filled = False
            sell_filled = False
            sell_was_limit = True  # 标记卖单是否为限价单
            
            while time.time() - start_time < self.order_timeout:
                # 检查买单状态
                if not buy_filled:
                    buy_status = buy_client.get_order(self.symbol, origClientOrderId=buy_order_id)
                    if buy_status.get('status') in ['FILLED', 'PARTIALLY_FILLED']:
                        buy_filled = True
                        print("市价买单已成交")
                
                # 检查卖单状态
                if not sell_filled:
                    sell_status = sell_client.get_order(self.symbol, origClientOrderId=sell_order_id)
                    if sell_status.get('status') in ['FILLED', 'PARTIALLY_FILLED']:
                        sell_filled = True
                        print("限价卖单已成交")
                        # 记录限价卖单成功
                        if sell_was_limit:
                            self.limit_sell_success_count += 1
                
                if buy_filled and sell_filled:
                    break
                    
                # 如果买单成交但卖单未成交，转为市价卖出
                if buy_filled and not sell_filled:
                    print("检测到风险: 买单成交但卖单未成交，转为市价卖出")
                    sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
                    
                    # 标记卖单已转为市价单
                    sell_was_limit = False
                    
                    # 立即下市价卖单，卖出实际持有的AT数量
                    emergency_sell_quantity, _ = self.get_sell_quantity()  # 重新获取当前可卖数量
                    if emergency_sell_quantity > 0:
                        emergency_sell = sell_client.create_order(
                            symbol=self.symbol,
                            side='SELL',
                            order_type='MARKET',
                            quantity=emergency_sell_quantity,  # 卖出实际持有的数量
                            newClientOrderId=f"emergency_sell_{timestamp}"
                        )
                        
                        if 'orderId' in emergency_sell:
                            print(f"紧急市价卖单已提交: 数量={emergency_sell_quantity:.4f}")
                            # 等待卖单成交
                            time.sleep(2)
                            sell_filled = True
                            # 记录市价卖单成功
                            self.market_sell_success_count += 1
                        else:
                            print("紧急市价卖单失败")
                            return False
                    else:
                        print("无可卖AT数量，无法进行紧急卖出")
                        return False
                
                time.sleep(0.5)
            
            # 清理未成交订单
            if not buy_filled:
                buy_client.cancel_order(self.symbol, origClientOrderId=buy_order_id)
            if not sell_filled and sell_was_limit:  # 只有限价单才需要取消
                sell_client.cancel_order(self.symbol, origClientOrderId=sell_order_id)
            
            success = buy_filled and sell_filled
            
            # 交易成功后更新缓存
            if success:
                self.update_cache_after_trade()
            
            return success
            
        except Exception as e:
            print(f"策略2执行出错: {e}")
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
                        print(f"订单 {order_id} 已成交")
                    elif order_status.get('status') in ['CANCELED', 'REJECTED', 'EXPIRED']:
                        print(f"订单 {order_id} 失败: {order_status.get('status')}")
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
        print("订单等待超时，取消未完成订单")
        for client, order_id in orders:
            if not any(c[1] == order_id and completed[i] for i, c in enumerate(orders)):
                client.cancel_order(self.symbol, origClientOrderId=order_id)
        
        return False
    
    def update_cache_after_trade(self):
        """交易成功后更新缓存数据"""
        print("🔄 交易成功，更新缓存数据...")
        
        # 强制刷新余额缓存
        self.client1.refresh_balance_cache()
        self.client2.refresh_balance_cache()
        
        # 更新交易方向缓存
        self.update_trade_direction_cache()
        
        print("✅ 缓存数据已更新")
    
    def update_cache_after_failure(self):
        """交易失败后更新缓存数据"""
        print("🔄 交易失败，更新缓存数据...")
        
        # 强制刷新余额缓存
        self.client1.refresh_balance_cache()
        self.client2.refresh_balance_cache()
        
        # 更新交易方向缓存
        self.update_trade_direction_cache()
        
        print("✅ 缓存数据已更新")
    
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
            print(f"✓ 交易成功! {sell_account}卖出 → {buy_account}买入")
            print(f"  本次交易量: {trade_volume:.4f}, 累计: {self.total_volume:.2f}/{self.target_volume}")
        else:
            print("✗ 交易失败")
            # 交易失败后也更新缓存
            self.update_cache_after_failure()
        
        return success
    
    def print_trading_statistics(self):
        """打印交易统计信息"""
        print("\n📊 交易统计信息:")
        print(f"   总尝试次数: {self.trade_count}")
        print(f"   成功交易次数: {self.successful_trades}")
        
        if self.trade_count > 0:
            success_rate = (self.successful_trades / self.trade_count) * 100
            print(f"   总体成功率: {success_rate:.1f}%")
        
        print(f"   卖单限价单尝试次数: {self.limit_sell_attempt_count}")
        print(f"   卖单限价单成功次数: {self.limit_sell_success_count}")
        
        if self.limit_sell_attempt_count > 0:
            limit_sell_success_rate = (self.limit_sell_success_count / self.limit_sell_attempt_count) * 100
            print(f"   卖单限价单成功率: {limit_sell_success_rate:.1f}%")
        
        print(f"   卖单市价单成功次数: {self.market_sell_success_count}")
        print(f"   累计交易量: {self.total_volume:.2f}/{self.target_volume}")
    
    def print_account_balances(self):
        """打印账户余额（使用缓存数据）"""
        try:
            # 使用缓存数据获取余额
            at_balance1 = self.client1.get_asset_balance(self.base_asset)
            usdt_balance1 = self.client1.get_asset_balance(self.quote_asset)
            
            at_balance2 = self.client2.get_asset_balance(self.base_asset)
            usdt_balance2 = self.client2.get_asset_balance(self.quote_asset)
            
            print(f"账户1: {self.base_asset}={at_balance1:.4f}, {self.quote_asset}={usdt_balance1:.2f}")
            print(f"账户2: {self.base_asset}={at_balance2:.4f}, {self.quote_asset}={usdt_balance2:.2f}")
            
            # 显示当前推荐交易方向
            sell_account, buy_account = self.get_current_trade_direction()
            print(f"推荐方向: {sell_account}卖出 → {buy_account}买入")
            
        except Exception as e:
            print(f"获取余额时出错: {e}")
    
    def monitor_and_trade(self):
        """监控市场并执行交易"""
        print("开始智能刷量交易...")
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
                        print("连续多次交易失败，暂停20秒...")
                        time.sleep(20)
                        consecutive_failures = 0
                
                # 显示进度
                progress = self.total_volume / self.target_volume * 100
                success_rate = (self.successful_trades / self.trade_count * 100) if self.trade_count > 0 else 0
                print(f"进度: {progress:.1f}% ({self.total_volume:.2f}/{self.target_volume}), 成功率: {success_rate:.1f}%")
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                print(f"交易周期出错: {e}")
                time.sleep(self.check_interval)
        
        if self.total_volume >= self.target_volume:
            print(f"🎉 达到目标交易量: {self.total_volume:.2f}")
        else:
            print("交易已停止")
    
    def start(self):
        """启动交易程序"""
        print("=" * 60)
        print("智能刷量交易程序启动 - 自动判断交易方向")
        print(f"交易对: {self.symbol}")
        print(f"基础资产: {self.base_asset}")
        print(f"策略: {self.strategy.value}")
        print(f"固定买单数量: {self.fixed_buy_quantity}")
        print(f"目标交易量: {self.target_volume}")
        print(f"价差阈值: {self.max_spread:.2%}")
        print(f"波动阈值: {self.max_price_change:.2%}")
        print("=" * 60)
        
        # 初始化缓存
        print("🔄 初始化缓存数据...")
        self.client1.refresh_balance_cache()
        self.client2.refresh_balance_cache()
        self.update_trade_direction_cache()
        print("✅ 缓存数据初始化完成")
        
        # 打印初始余额和推荐方向
        print("初始账户余额和推荐交易方向:")
        self.print_account_balances()
        print()
        
        # 启动交易
        self.monitor_and_trade()
    
    def stop(self):
        """停止交易"""
        self.is_running = False
        print("\n交易程序已停止")
        print("=" * 50)
        print("最终交易统计:")
        self.print_trading_statistics()
        print("=" * 50)
        print("最终账户余额:")
        self.print_account_balances()

def main():
    """主函数"""
    maker = SmartMarketMaker()
    
    try:
        maker.start()
    except KeyboardInterrupt:
        print("\n收到停止信号...")
    except Exception as e:
        print(f"程序运行出错: {e}")
    finally:
        maker.stop()

if __name__ == "__main__":
    main()