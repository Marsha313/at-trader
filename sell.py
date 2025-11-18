import requests
import time
import hmac
import hashlib
import urllib.parse
import math
from typing import Dict, List, Optional, Tuple
import os
from dotenv import load_dotenv
import logging
import sys
from datetime import datetime
import argparse

# 设置日志
def setup_logging(log_filename=None):
    """设置日志配置"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    if log_filename is None:
        log_filename = f"logs/cleanup_mode_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    else:
        if not log_filename.startswith('logs/'):
            log_filename = f"logs/{log_filename}"
        if not log_filename.endswith('.log'):
            log_filename += '.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"📝 日志文件: {log_filename}")
    
    return logger

class AsterDexClient:
    def __init__(self, api_key: str, secret_key: str, account_name: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.account_name = account_name
        self.base_url = os.getenv('BASE_URL', 'https://sapi.asterdex.com')
        self._balance_cache = None
        self.logger = logging.getLogger(f"{__name__}.{account_name}")
        
    def _sign_request(self, params: Dict) -> str:
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False) -> Dict:
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
            if str(e).find('Too Many Requests') != -1:
                self.logger.error("请求过多，可能被限流,等待30s")
                time.sleep(30)
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"错误响应: {e.response.text}")
            return {'error': str(e),'text': getattr(e.response, 'text', '')}
    
    def create_order(self, symbol: str, side: str, order_type: str, 
                    quantity: float, min_price_increment:float,price: Optional[float] = None) -> Dict:
        """创建订单 - 使用服务器生成的订单ID"""
        endpoint = "/api/v1/order"
        
        formatted_quantity = round(math.floor(quantity / 0.01) * 0.01, 2)
        
        formatted_price = None
        if price is not None and order_type != 'MARKET':
            num_length = 0
            change = min_price_increment
            while change < 1:
                num_length += 1
                change = change * 10
            formatted_price = round(price, num_length)
        
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': formatted_quantity
        }
        
        if formatted_price is not None:
            params['price'] = formatted_price
            params['timeInForce'] = 'GTC'
        
        self.logger.info(f"📤 发送订单请求:")
        self.logger.info(f"   交易对: {symbol}")
        self.logger.info(f"   方向: {side}")
        self.logger.info(f"   类型: {order_type}")
        self.logger.info(f"   数量: {quantity} -> {formatted_quantity}")
        if formatted_price:
            self.logger.info(f"   价格: {price} -> {formatted_price}")
        
        return self._request('POST', endpoint, params, signed=True)
    
    def cancel_order(self, symbol: str, order_id: int) -> Dict:
        """取消订单 - 使用服务器订单ID，如果取消失败则当作订单已成交"""
        endpoint = "/api/v1/order"
        params = {
            'symbol': symbol,
            'orderId': order_id
        }
            
        result = self._request('DELETE', endpoint, params, signed=True)
        
        # 如果取消失败，检查是否是订单已成交的情况
        if 'error' in result or 'code' in result:
            error_msg = str(result.get('error', result.get('msg', 'Unknown error'))) + str(result.get('text', ''))
            
            # 如果错误信息表明订单不存在或已成交，当作订单已成交处理
            if any(keyword in error_msg for keyword in ['does not exist', 'not found', 'already filled', 'filled']):
                self.logger.info(f"⚠️ 取消订单失败，订单可能已成交: {error_msg}")
                # 返回一个模拟的成功响应，表示订单已成交
                return {'orderId': order_id, 'status': 'FILLED'}
            else:
                self.logger.error(f"❌ 取消订单失败: {error_msg}")
        
        return result
    
    def get_order(self, symbol: str, order_id: int) -> Dict:
        """查询订单状态 - 使用服务器订单ID"""
        endpoint = "/api/v1/order"
        params = {
            'symbol': symbol,
            'orderId': order_id
        }
            
        return self._request('GET', endpoint, params, signed=True)
    
    def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """获取当前挂单"""
        endpoint = "/api/v1/openOrders"
        params = {}
        if symbol:
            params['symbol'] = symbol
        
        data = self._request('GET', endpoint, params, signed=True)
        
        if isinstance(data, list):
            return data
        else:
            self.logger.error(f"获取挂单失败: {data}")
            return []

    def cancel_all_orders(self, symbol: str = None) -> bool:
        """取消指定交易对的所有挂单"""
        try:
            open_orders = self.get_open_orders(symbol)
            if not open_orders:
                self.logger.info(f"✅ {self.account_name} 没有需要取消的挂单")
                return True
            
            self.logger.info(f"🔄 {self.account_name} 开始取消 {len(open_orders)} 个挂单")
            success_count = 0
            
            for order in open_orders:
                order_id = order.get('orderId')
                order_symbol = order.get('symbol')
                
                try:
                    cancel_result = self.cancel_order(order_symbol, order_id)
                    
                    if 'orderId' in cancel_result:
                        success_count += 1
                        self.logger.info(f"✅ 取消挂单成功: {order_symbol} - {order_id}")
                    else:
                        self.logger.error(f"❌ 取消挂单失败: {order_symbol} - {order_id}: {cancel_result}")
                        
                except Exception as e:
                    self.logger.error(f"❌ 取消挂单异常: {order_symbol} - {order_id}: {e}")
            
            self.logger.info(f"📊 {self.account_name} 取消挂单完成: 成功 {success_count}/{len(open_orders)}")
            return success_count == len(open_orders)
            
        except Exception as e:
            self.logger.error(f"❌ 取消所有挂单时出错: {e}")
            return False
    
    def get_order_book(self, symbol: str, limit: int = 10) -> Dict:
        """获取订单簿"""
        endpoint = "/api/v1/depth"
        params = {
            'symbol': symbol,
            'limit': limit
        }
        data = self._request('GET', endpoint, params)
        
        if not data or 'bids' not in data:
            return {'bids': [], 'asks': []}
            
        bids = [[float(bid[0]), float(bid[1])] for bid in data.get('bids', [])]
        asks = [[float(ask[0]), float(ask[1])] for ask in data.get('asks', [])]
        
        return {'bids': bids, 'asks': asks}
    
    def get_account_balance(self, force_refresh: bool = False) -> Dict[str, Dict]:
        """获取账户余额"""
        if self._balance_cache is not None and not force_refresh:
            return self._balance_cache
        
        endpoint = "/api/v1/account"
        data = self._request('GET', endpoint, signed=True)
        
        balances = {}
        if 'balances' in data:
            for balance in data['balances']:
                asset = balance['asset']
                balances[asset] = {
                    'free': float(balance.get('free', 0)),
                    'locked': float(balance.get('locked', 0))
                }
        
        self._balance_cache = balances
        return balances
    
    def get_asset_balance(self, asset: str, force_refresh: bool = False) -> float:
        """获取指定资产的可用余额"""
        balances = self.get_account_balance(force_refresh)
        if asset in balances:
            return balances[asset]['free'] + balances[asset]['locked']
        return 0.0
    
    def refresh_balance_cache(self):
        """强制刷新余额缓存"""
        self._balance_cache = None
        return self.get_account_balance(force_refresh=True)

class CleanupMode:
    def __init__(self, config_file: str = ".env", log_filename: str = None):
        self.config_file = config_file
        self.monitoring_orders = {}  # 存储监控中的订单
        self.is_monitoring = False   # 监控状态
        
        if os.path.exists(config_file):
            load_dotenv(config_file)
            self.logger = setup_logging(log_filename)
            self.logger.info(f"📁 使用配置文件: {config_file}")
        else:
            self.logger = setup_logging(log_filename)
            self.logger.warning(f"⚠️ 配置文件 {config_file} 不存在，使用默认配置")
        
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
        
        self.trading_pairs = self.load_trading_pairs_config()
        
    def load_trading_pairs_config(self) -> List[Dict]:
        """加载多交易对配置"""
        pairs_config = []
        
        pairs_str = os.getenv('TRADING_PAIRS', 'ATUSDT,BTTCUSDT')
        pairs_list = [pair.strip() for pair in pairs_str.split(',')]
        
        for pair_symbol in pairs_list:
            base_asset = pair_symbol.replace('USDT', '')
            min_price_increment = float(os.getenv(f'{base_asset}_MIN_PRICE_INCREMENT', 0.0001))
            
            pair_config = {
                'symbol': pair_symbol,
                'base_asset': base_asset,
                'min_price_increment': min_price_increment
            }
            pairs_config.append(pair_config)
            
            self.logger.info(f"📋 加载交易对配置: {pair_symbol}")
            self.logger.info(f"   基础资产: {base_asset}")
            self.logger.info(f"   最小价格变动单位: {min_price_increment}")
        
        return pairs_config

    def filter_trading_pairs(self, specified_tokens: List[str]) -> List[Dict]:
        """根据指定的代币过滤交易对"""
        if not specified_tokens:
            return self.trading_pairs
        
        filtered_pairs = []
        for token in specified_tokens:
            token_upper = token.upper()
            # 检查是否包含USDT后缀
            if not token_upper.endswith('USDT'):
                token_upper += 'USDT'
            
            # 查找匹配的交易对
            matched = False
            for pair in self.trading_pairs:
                if pair['symbol'] == token_upper or pair['base_asset'].upper() == token.upper():
                    filtered_pairs.append(pair)
                    matched = True
                    self.logger.info(f"✅ 找到匹配的交易对: {pair['symbol']}")
                    break
            
            if not matched:
                self.logger.warning(f"⚠️ 未找到代币 {token} 的配置，将使用默认配置创建")
                # 为未配置的代币创建默认配置
                default_pair = {
                    'symbol': token_upper,
                    'base_asset': token.upper().replace('USDT', ''),
                    'min_price_increment': 0.0001  # 默认最小价格变动
                }
                filtered_pairs.append(default_pair)
                self.logger.info(f"📋 为 {token} 创建默认配置")
        
        return filtered_pairs

    def format_price(self, price: float, min_increment: float) -> float:
        """根据最小价格变动单位格式化价格"""
        if min_increment <= 0:
            return round(price, 6)
        
        precision = self.get_price_precision(min_increment)
        return round(price, precision)

    def get_price_precision(self, min_increment: float) -> int:
        """根据最小价格变动单位计算精度位数"""
        if min_increment >= 1:
            return 0
        elif min_increment >= 0.1:
            return 1
        elif min_increment >= 0.01:
            return 2
        elif min_increment >= 0.001:
            return 3
        elif min_increment >= 0.0001:
            return 4
        elif min_increment >= 0.00001:
            return 5
        elif min_increment >= 0.000001:
            return 6
        else:
            return 8

    def cancel_all_open_orders(self, specified_pairs: List[Dict] = None):
        """取消指定交易对的挂单"""
        pairs_to_cancel = specified_pairs if specified_pairs else self.trading_pairs
        symbols = [pair['symbol'] for pair in pairs_to_cancel]
        
        self.logger.info("🔄 开始取消相关交易对的挂单...")
        self.logger.info(f"📋 需要清理的交易对: {', '.join(symbols)}")
        
        success1 = True
        success2 = True
        
        for symbol in symbols:
            self.logger.info(f"🔄 清理交易对 {symbol} 的挂单...")
            success1 = success1 and self.client1.cancel_all_orders(symbol)
            success2 = success2 and self.client2.cancel_all_orders(symbol)
        
        if success1 and success2:
            self.logger.info("✅ 所有挂单清理完成")
        else:
            self.logger.warning("⚠️ 部分挂单清理可能失败，但程序将继续运行")
        
        time.sleep(2)

    def get_sell_price(self, pair: Dict, custom_price: float = None) -> float:
        """获取卖出价格"""
        if custom_price is not None:
            self.logger.info(f"🎯 使用自定义卖出价格: {custom_price:.6f}")
            return self.format_price(custom_price, pair['min_price_increment'])
        
        # 获取市场价格
        try:
            order_book = self.client1.get_order_book(pair['symbol'], limit=5)
            if not order_book['bids'] or not order_book['asks']:
                self.logger.error(f"❌ 无法获取 {pair['symbol']} 的市场价格")
                return None
            
            best_bid = order_book['bids'][0][0]
            best_ask = order_book['asks'][0][0]
            self.logger.info(f"   当前市场: 买一={best_bid:.6f}, 卖一={best_ask:.6f}")
            
            # 默认使用卖一价格减一个最小变动单位（确保快速成交）
            sell_price = self.format_price(best_ask - pair['min_price_increment'], pair['min_price_increment'])
            if sell_price <= best_bid:
                sell_price = self.format_price(best_bid + pair['min_price_increment'], pair['min_price_increment'])
            
            self.logger.info(f"   自动设置卖出价格: {sell_price:.6f}")
            return sell_price
            
        except Exception as e:
            self.logger.error(f"❌ 获取 {pair['symbol']} 市场数据失败: {e}")
            return None

    def create_limit_sell_orders(self, specified_pairs: List[Dict], custom_price: float = None) -> List[Dict]:
        """创建限价卖单，返回订单信息列表"""
        self.logger.info("🔄 开始创建限价卖单...")
        
        orders_to_monitor = []
        
        for pair in specified_pairs:
            self.logger.info(f"\n📊 处理交易对: {pair['symbol']}")
            
            # 获取卖出价格
            sell_price = self.get_sell_price(pair, custom_price)
            if sell_price is None:
                self.logger.error(f"❌ 无法确定 {pair['symbol']} 的卖出价格，跳过")
                continue
            
            # 检查两个账户的代币余额并挂卖单
            for client, client_name in [(self.client1, 'ACCOUNT1'), (self.client2, 'ACCOUNT2')]:
                try:
                    asset_balance = client.get_asset_balance(pair['base_asset'])
                    if asset_balance > 0:
                        self.logger.info(f"   {client_name} {pair['base_asset']} 余额: {asset_balance:.4f}")
                        
                        # 挂限价卖单
                        sell_order = client.create_order(
                            symbol=pair['symbol'],
                            side='SELL',
                            order_type='LIMIT',
                            quantity=asset_balance,
                            min_price_increment=pair['min_price_increment'],
                            price=sell_price
                        )
                        
                        if 'orderId' in sell_order:
                            order_info = {
                                'client': client,
                                'client_name': client_name,
                                'symbol': pair['symbol'],
                                'order_id': sell_order['orderId'],
                                'original_price': sell_price,
                                'current_price': sell_price,
                                'quantity': asset_balance,
                                'base_asset': pair['base_asset'],
                                'min_price_increment': pair['min_price_increment'],
                                'status': 'NEW',
                                'create_time': time.time()
                            }
                            orders_to_monitor.append(order_info)
                            
                            self.logger.info(f"   ✅ {client_name} 限价卖单挂出成功:")
                            self.logger.info(f"      数量: {asset_balance:.4f} {pair['base_asset']}")
                            self.logger.info(f"      价格: {sell_price:.6f} USDT")
                            self.logger.info(f"      订单ID: {sell_order['orderId']}")
                        else:
                            self.logger.error(f"   ❌ {client_name} 限价卖单失败: {sell_order}")
                    else:
                        self.logger.info(f"   ℹ️  {client_name} 没有 {pair['base_asset']} 余额")
                        
                except Exception as e:
                    self.logger.error(f"   ❌ {client_name} 处理 {pair['base_asset']} 卖出时出错: {e}")
        
        return orders_to_monitor

    def get_current_market_price(self, symbol: str) -> Tuple[float, float]:
        """获取当前市场价格"""
        try:
            order_book = self.client1.get_order_book(symbol, limit=5)
            if not order_book['bids'] or not order_book['asks']:
                return 0, 0
            
            best_bid = order_book['bids'][0][0]
            best_ask = order_book['asks'][0][0]
            return best_bid, best_ask
            
        except Exception as e:
            self.logger.error(f"获取 {symbol} 市场数据失败: {e}")
            return 0, 0

    def check_order_status(self, order_info: Dict) -> str:
        """检查订单状态"""
        try:
            order_status = order_info['client'].get_order(order_info['symbol'], order_info['order_id'])
            status = order_status.get('status', 'UNKNOWN')
            executed_qty = float(order_status.get('executedQty', 0))
            
            # 更新订单信息
            order_info['status'] = status
            order_info['executed_qty'] = executed_qty
            
            return status
            
        except Exception as e:
            self.logger.error(f"检查订单 {order_info['order_id']} 状态失败: {e}")
            return 'UNKNOWN'

    def should_adjust_price(self, order_info: Dict, current_ask: float) -> bool:
        """判断是否需要调整价格"""
        if current_ask == 0:
            return False
        
        # 如果当前价格高于卖一价格，需要调整
        if order_info['current_price'] > current_ask + order_info['min_price_increment']:
            return True
        
        return False

    def adjust_order_price(self, order_info: Dict, new_price: float) -> bool:
        """调整订单价格"""
        try:
            self.logger.info(f"🔄 调整订单 {order_info['order_id']} 价格: {order_info['current_price']:.6f} -> {new_price:.6f}")
            
            # 先取消原订单
            cancel_result = order_info['client'].cancel_order(order_info['symbol'], order_info['order_id'])
            
            if 'orderId' not in cancel_result and cancel_result.get('status') != 'FILLED':
                self.logger.error(f"❌ 取消订单 {order_info['order_id']} 失败")
                return False
            
            # 如果订单已成交，返回成功
            if cancel_result.get('status') == 'FILLED':
                self.logger.info(f"✅ 取消订单时发现订单已完全成交")
                order_info['status'] = 'FILLED'
                return True
            
            # 计算剩余数量
            remaining_qty = order_info['quantity'] - order_info.get('executed_qty', 0)
            if remaining_qty <= 0:
                self.logger.info(f"✅ 订单已完全成交")
                order_info['status'] = 'FILLED'
                return True
            
            # 重新挂单
            new_order = order_info['client'].create_order(
                symbol=order_info['symbol'],
                side='SELL',
                order_type='LIMIT',
                quantity=remaining_qty,
                min_price_increment=order_info['min_price_increment'],
                price=new_price
            )
            
            if 'orderId' in new_order:
                # 更新订单信息
                order_info['order_id'] = new_order['orderId']
                order_info['current_price'] = new_price
                order_info['quantity'] = remaining_qty
                order_info['status'] = 'NEW'
                order_info['create_time'] = time.time()
                
                self.logger.info(f"✅ 订单价格调整成功，新订单ID: {new_order['orderId']}")
                return True
            else:
                self.logger.error(f"❌ 重新挂单失败: {new_order}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 调整订单价格时出错: {e}")
            return False

    def monitor_orders_until_filled(self, orders_to_monitor: List[Dict], max_monitor_time: int = 3600):
        """监控订单直到完全成交"""
        self.logger.info(f"\n🔄 开始监控订单，最大监控时间: {max_monitor_time}秒")
        self.is_monitoring = True
        
        start_time = time.time()
        check_interval = 5  # 检查间隔（秒）
        
        while self.is_monitoring and time.time() - start_time < max_monitor_time:
            active_orders = [order for order in orders_to_monitor if order['status'] not in ['FILLED', 'CANCELED', 'REJECTED']]
            
            if not active_orders:
                self.logger.info("✅ 所有订单都已成交或取消")
                break
            
            self.logger.info(f"\n📊 当前监控中的订单: {len(active_orders)} 个")
            
            for order_info in active_orders:
                # 检查订单状态
                status = self.check_order_status(order_info)
                
                if status == 'FILLED':
                    self.logger.info(f"✅ {order_info['client_name']} - {order_info['symbol']} 订单已完全成交")
                    continue
                
                elif status in ['CANCELED', 'REJECTED']:
                    self.logger.warning(f"⚠️ {order_info['client_name']} - {order_info['symbol']} 订单状态: {status}")
                    continue
                
                # 获取当前市场价格
                current_bid, current_ask = self.get_current_market_price(order_info['symbol'])
                
                if current_ask > 0:
                    # 检查是否需要调整价格
                    if self.should_adjust_price(order_info, current_ask):
                        # 计算新价格（卖一价格减一个最小变动单位）
                        new_price = self.format_price(current_ask - order_info['min_price_increment'], order_info['min_price_increment'])
                        if new_price <= current_bid:
                            new_price = self.format_price(current_bid + order_info['min_price_increment'], order_info['min_price_increment'])
                        
                        self.logger.info(f"🔄 {order_info['client_name']} - {order_info['symbol']} 价格需要调整")
                        self.logger.info(f"   当前订单价格: {order_info['current_price']:.6f}")
                        self.logger.info(f"   当前卖一价格: {current_ask:.6f}")
                        self.logger.info(f"   新价格: {new_price:.6f}")
                        
                        # 调整价格
                        self.adjust_order_price(order_info, new_price)
                    
                    else:
                        # 显示订单状态
                        executed_qty = order_info.get('executed_qty', 0)
                        fill_rate = (executed_qty / order_info['quantity']) * 100 if order_info['quantity'] > 0 else 0
                        
                        self.logger.info(f"📊 {order_info['client_name']} - {order_info['symbol']}:")
                        self.logger.info(f"   状态: {status}")
                        self.logger.info(f"   成交: {executed_qty:.4f}/{order_info['quantity']:.4f} ({fill_rate:.1f}%)")
                        self.logger.info(f"   价格: {order_info['current_price']:.6f} (卖一: {current_ask:.6f})")
                
                else:
                    self.logger.warning(f"⚠️ 无法获取 {order_info['symbol']} 的市场价格")
            
            # 等待下一次检查
            if self.is_monitoring and any(order['status'] not in ['FILLED', 'CANCELED', 'REJECTED'] for order in orders_to_monitor):
                elapsed_time = time.time() - start_time
                self.logger.info(f"⏰ 已监控 {elapsed_time:.0f} 秒，{check_interval} 秒后继续检查...")
                time.sleep(check_interval)
        
        # 监控结束
        if time.time() - start_time >= max_monitor_time:
            self.logger.warning(f"⏰ 达到最大监控时间 {max_monitor_time} 秒，停止监控")
        
        # 显示最终结果
        self.show_monitoring_results(orders_to_monitor)

    def show_monitoring_results(self, orders_to_monitor: List[Dict]):
        """显示监控结果"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 订单监控最终结果:")
        
        filled_orders = [o for o in orders_to_monitor if o['status'] == 'FILLED']
        active_orders = [o for o in orders_to_monitor if o['status'] not in ['FILLED', 'CANCELED', 'REJECTED']]
        failed_orders = [o for o in orders_to_monitor if o['status'] in ['CANCELED', 'REJECTED']]
        
        self.logger.info(f"   总订单数: {len(orders_to_monitor)}")
        self.logger.info(f"   已成交: {len(filled_orders)}")
        self.logger.info(f"   进行中: {len(active_orders)}")
        self.logger.info(f"   失败/取消: {len(failed_orders)}")
        
        if filled_orders:
            self.logger.info("\n✅ 已成交订单:")
            for order in filled_orders:
                self.logger.info(f"   {order['client_name']} - {order['symbol']}: {order['quantity']:.4f} @ {order['current_price']:.6f}")
        
        if active_orders:
            self.logger.info("\n🔄 进行中订单:")
            for order in active_orders:
                executed_qty = order.get('executed_qty', 0)
                fill_rate = (executed_qty / order['quantity']) * 100 if order['quantity'] > 0 else 0
                self.logger.info(f"   {order['client_name']} - {order['symbol']}: {executed_qty:.4f}/{order['quantity']:.4f} ({fill_rate:.1f}%) @ {order['current_price']:.6f}")
        
        if failed_orders:
            self.logger.info("\n❌ 失败/取消订单:")
            for order in failed_orders:
                self.logger.info(f"   {order['client_name']} - {order['symbol']}: {order['status']}")

    def show_open_orders(self, specified_pairs: List[Dict] = None):
        """显示当前挂单"""
        pairs_to_show = specified_pairs if specified_pairs else self.trading_pairs
        symbols = [pair['symbol'] for pair in pairs_to_show]
        
        self.logger.info("\n📋 当前挂单列表:")
        has_orders = False
        
        for symbol in symbols:
            for client, client_name in [(self.client1, 'ACCOUNT1'), (self.client2, 'ACCOUNT2')]:
                try:
                    open_orders = client.get_open_orders(symbol)
                    if open_orders:
                        has_orders = True
                        self.logger.info(f"\n   {client_name} - {symbol}:")
                        for order in open_orders:
                            self.logger.info(f"      订单ID: {order.get('orderId')}")
                            self.logger.info(f"      方向: {order.get('side')}")
                            self.logger.info(f"      类型: {order.get('type')}")
                            self.logger.info(f"      数量: {float(order.get('origQty', 0)):.4f}")
                            self.logger.info(f"      价格: {float(order.get('price', 0)):.6f}")
                            self.logger.info(f"      状态: {order.get('status', 'UNKNOWN')}")
                            self.logger.info(f"      ---")
                except Exception as e:
                    self.logger.error(f"   获取 {client_name} {symbol} 挂单失败: {e}")
        
        if not has_orders:
            self.logger.info("   ℹ️  当前没有挂单")

    def show_account_balances(self, specified_tokens: List[str] = None):
        """显示账户余额"""
        self.logger.info("\n💰 账户余额:")
        
        try:
            # 刷新余额缓存
            self.client1.refresh_balance_cache()
            self.client2.refresh_balance_cache()
            
            for client, client_name in [(self.client1, 'ACCOUNT1'), (self.client2, 'ACCOUNT2')]:
                self.logger.info(f"\n   {client_name}:")
                
                # 显示USDT余额
                usdt_balance = client.get_asset_balance('USDT')
                self.logger.info(f"      USDT: {usdt_balance:.2f}")
                
                # 显示指定代币或所有代币余额
                tokens_to_show = specified_tokens if specified_tokens else [pair['base_asset'] for pair in self.trading_pairs]
                
                for token in tokens_to_show:
                    asset_balance = client.get_asset_balance(token)
                    if asset_balance > 0:
                        self.logger.info(f"      {token}: {asset_balance:.4f}")
            
        except Exception as e:
            self.logger.error(f"获取余额时出错: {e}")

    def stop_monitoring(self):
        """停止监控"""
        self.is_monitoring = False
        self.logger.info("🛑 停止订单监控")

    def run_cleanup(self, specified_tokens: List[str] = None, custom_price: float = None, 
                   monitor: bool = True, show_balances: bool = False, 
                   show_orders: bool = False, max_monitor_time: int = 3600):
        """运行清理模式"""
        # 过滤交易对
        specified_pairs = self.filter_trading_pairs(specified_tokens)
        
        if not specified_pairs:
            self.logger.error("❌ 没有找到有效的交易对配置")
            return
        
        self.logger.info("🧹 启动清理模式...")
        self.logger.info("=" * 60)
        self.logger.info("清理模式操作:")
        self.logger.info(f"1. 处理代币: {', '.join([pair['base_asset'] for pair in specified_pairs])}")
        self.logger.info("2. 取消相关交易对的挂单")
        if custom_price is not None:
            self.logger.info(f"3. 以自定义价格 {custom_price:.6f} 挂限价单卖出指定代币")
        else:
            self.logger.info("3. 以卖一价格挂限价单卖出指定代币")
        if monitor:
            self.logger.info("4. 持续监控订单直到完全成交")
            self.logger.info(f"   最大监控时间: {max_monitor_time} 秒")
        if show_balances:
            self.logger.info("5. 显示账户余额")
        if show_orders:
            self.logger.info("6. 显示当前挂单")
        self.logger.info("=" * 60)
        
        try:
            # 第一步：取消所有挂单
            self.logger.info("🔄 第一步：取消相关交易对的挂单...")
            self.cancel_all_open_orders(specified_pairs)
            
            # 第二步：刷新余额缓存
            self.logger.info("🔄 第二步：刷新账户余额...")
            self.client1.refresh_balance_cache()
            self.client2.refresh_balance_cache()
            
            # 第三步：创建限价卖单
            self.logger.info("🔄 第三步：开始挂限价卖单...")
            orders_to_monitor = self.create_limit_sell_orders(specified_pairs, custom_price)
            
            if not orders_to_monitor:
                self.logger.info("ℹ️  没有需要卖出的代币余额")
                return
            
            # 第四步：持续监控订单
            if monitor:
                self.monitor_orders_until_filled(orders_to_monitor, max_monitor_time)
            
            # 第五步：显示账户余额（如果启用）
            if show_balances:
                token_list = [pair['base_asset'] for pair in specified_pairs]
                self.show_account_balances(token_list)
            
            # 第六步：显示当前挂单（如果启用）
            if show_orders:
                self.show_open_orders(specified_pairs)
                
        except KeyboardInterrupt:
            self.logger.info("\n🛑 收到停止信号")
            self.stop_monitoring()
        except Exception as e:
            self.logger.error(f"❌ 程序运行出错: {e}")
            self.stop_monitoring()

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='清理模式 - 取消所有订单并挂限价卖单')
    parser.add_argument('-c', '--config', type=str, default='.env', 
                       help='配置文件路径 (默认: .env)')
    parser.add_argument('-t', '--tokens', type=str, nargs='+', metavar='TOKEN',
                       help='指定要处理的代币 (例如: AT BTTC 或 ATUSDT BTTCUSDT)')
    parser.add_argument('-p', '--price', type=float, metavar='PRICE',
                       help='自定义卖出价格 (如未设置则使用卖一价格)')
    parser.add_argument('--no-monitor', action='store_true',
                       help='不监控订单 (默认会监控直到成交)')
    parser.add_argument('--monitor-time', type=int, default=3600,
                       help='最大监控时间 (秒) (默认: 3600)')
    parser.add_argument('--show-balances', action='store_true',
                       help='显示账户余额')
    parser.add_argument('--show-orders', action='store_true',
                       help='显示当前挂单')
    parser.add_argument('--log', type=str, metavar='FILENAME',
                       help='自定义日志文件名 (不需要.log后缀)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print(f"错误: 配置文件 {args.config} 不存在")
        return
    
    # 创建清理模式实例
    cleanup = CleanupMode(config_file=args.config, log_filename=args.log)
    
    try:
        # 运行清理模式
        cleanup.run_cleanup(
            specified_tokens=args.tokens,
            custom_price=args.price,
            monitor=not args.no_monitor,
            show_balances=args.show_balances,
            show_orders=args.show_orders,
            max_monitor_time=args.monitor_time
        )
    except KeyboardInterrupt:
        cleanup.logger.info("\n程序退出")
    except Exception as e:
        cleanup.logger.error(f"程序运行出错: {e}")

if __name__ == "__main__":
    main()