import os
from dotenv import load_dotenv
import logging
from typing import Dict, List
from market_maker import AsterDexClient
import sys
from datetime import datetime
import json
import time

def setup_logging():
    """设置日志配置"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    log_filename = f"logs/volume_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

class TradeDataCache:
    """交易数据缓存管理"""
    
    def __init__(self, cache_dir: str = "trade_cache"):
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
    
    def get_trades_cache_file(self, account_name: str, symbol: str) -> str:
        """获取交易记录缓存文件路径"""
        safe_symbol = symbol.replace('/', '_')
        return os.path.join(self.cache_dir, f"{account_name}_{safe_symbol}_trades.json")
    
    def get_stats_cache_file(self) -> str:
        """获取统计结果缓存文件路径"""
        return os.path.join(self.cache_dir, "volume_stats_cache.json")
    
    def get_balance_cache_file(self, account_name: str) -> str:
        """获取余额缓存文件路径"""
        return os.path.join(self.cache_dir, f"{account_name}_balance.json")
    
    def get_price_cache_file(self) -> str:
        """获取价格缓存文件路径"""
        return os.path.join(self.cache_dir, "price_cache.json")
    
    def load_cached_trades(self, account_name: str, symbol: str) -> List[Dict]:
        """从缓存加载交易记录"""
        cache_file = self.get_trades_cache_file(account_name, symbol)
        
        if not os.path.exists(cache_file):
            return []
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('trades', [])
        except Exception as e:
            logging.warning(f"加载交易缓存失败 {account_name} {symbol}: {e}")
            return []
    
    def save_trades_to_cache(self, account_name: str, symbol: str, trades: List[Dict]):
        """保存交易记录到缓存"""
        cache_file = self.get_trades_cache_file(account_name, symbol)
        
        try:
            cache_data = {
                'symbol': symbol,
                'account_name': account_name,
                'last_updated': datetime.now().isoformat(),
                'total_trades': len(trades),
                'trades': trades
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"✅ 交易缓存保存成功: {account_name} {symbol} ({len(trades)} 笔交易)")
            
        except Exception as e:
            logging.error(f"保存交易缓存失败 {account_name} {symbol}: {e}")
    
    def load_cached_stats(self) -> Dict:
        """从缓存加载统计结果"""
        cache_file = self.get_stats_cache_file()
        
        if not os.path.exists(cache_file):
            return {}
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"加载统计缓存失败: {e}")
            return {}
    
    def save_stats_to_cache(self, stats_data: Dict):
        """保存统计结果到缓存"""
        cache_file = self.get_stats_cache_file()
        
        try:
            cache_data = {
                'last_updated': datetime.now().isoformat(),
                'stats': stats_data
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logging.info("✅ 统计缓存保存成功")
            
        except Exception as e:
            logging.error(f"保存统计缓存失败: {e}")
    
    def load_cached_balance(self, account_name: str) -> Dict:
        """从缓存加载账户余额"""
        cache_file = self.get_balance_cache_file(account_name)
        
        if not os.path.exists(cache_file):
            return {}
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('balances', {})
        except Exception as e:
            logging.warning(f"加载余额缓存失败 {account_name}: {e}")
            return {}
    
    def save_balance_to_cache(self, account_name: str, balances: Dict):
        """保存账户余额到缓存"""
        cache_file = self.get_balance_cache_file(account_name)
        
        try:
            cache_data = {
                'account_name': account_name,
                'last_updated': datetime.now().isoformat(),
                'balances': balances
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"✅ 余额缓存保存成功: {account_name}")
            
        except Exception as e:
            logging.error(f"保存余额缓存失败 {account_name}: {e}")
    
    def load_cached_prices(self) -> Dict:
        """从缓存加载价格数据"""
        cache_file = self.get_price_cache_file()
        
        if not os.path.exists(cache_file):
            return {}
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 检查缓存是否过期（5分钟）
                last_updated = datetime.fromisoformat(data.get('last_updated', '2000-01-01'))
                if (datetime.now() - last_updated).total_seconds() < 300:  # 5分钟
                    return data.get('prices', {})
                else:
                    logging.info("价格缓存已过期，重新获取")
                    return {}
        except Exception as e:
            logging.warning(f"加载价格缓存失败: {e}")
            return {}
    
    def save_prices_to_cache(self, prices: Dict):
        """保存价格数据到缓存"""
        cache_file = self.get_price_cache_file()
        
        try:
            cache_data = {
                'last_updated': datetime.now().isoformat(),
                'prices': prices
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logging.info("✅ 价格缓存保存成功")
            
        except Exception as e:
            logging.error(f"保存价格缓存失败: {e}")
    
    def get_latest_trade_id(self, account_name: str, symbol: str) -> int:
        """获取缓存中最大的交易ID"""
        cached_trades = self.load_cached_trades(account_name, symbol)
        if not cached_trades:
            return 0
        
        try:
            return max(int(trade.get('id', 0)) for trade in cached_trades)
        except:
            return 0
    
    def merge_trades(self, old_trades: List[Dict], new_trades: List[Dict]) -> List[Dict]:
        """合并新旧交易记录，去重"""
        if not old_trades:
            return new_trades
        
        if not new_trades:
            return old_trades
        
        # 创建交易ID映射
        trade_dict = {trade['id']: trade for trade in old_trades}
        
        # 添加新交易，覆盖重复的
        for trade in new_trades:
            trade_dict[trade['id']] = trade
        
        # 按交易ID排序
        merged_trades = sorted(trade_dict.values(), key=lambda x: int(x['id']))
        return merged_trades

class VolumeStatistics:
    """交易量统计程序（带缓存功能）"""
    
    def __init__(self):
        # 加载环境变量
        load_dotenv('account.env')
        
        # 设置日志
        self.logger = setup_logging()
        
        # 初始化缓存
        self.cache = TradeDataCache()
        
        # 初始化客户端（使用第一个账户获取价格）
        self.clients = {}
        self.price_client = None
        self.init_clients()
        
        # 配置要统计的代币
        self.tokens_to_track = self.load_tokens_config()
        
        # 统计结果
        self.volume_stats = {}
        self.balance_stats = {}
        self.current_prices = {}
        
        # 缓存统计
        self.cache_stats = {
            'cached_trades': 0,
            'new_trades': 0,
            'api_calls_made': 0,
            'api_calls_saved': 0
        }
    
    def load_tokens_config(self) -> List[str]:
        """加载要统计的代币配置"""
        tokens_str = os.getenv('TRACK_TOKENS', 'ATUSDT,BTTCUSDT,ASTERUSDT')
        tokens_list = [token.strip() for token in tokens_str.split(',')]
        
        self.logger.info(f"📋 配置统计的代币: {', '.join(tokens_list)}")
        return tokens_list
    
    def init_clients(self):
        """初始化所有账户客户端"""
        # 从环境变量读取账户配置
        account_count = int(os.getenv('ACCOUNT_COUNT', 2))
        
        for i in range(1, account_count + 1):
            api_key = os.getenv(f'ACCOUNT_{i}_API_KEY')
            secret_key = os.getenv(f'ACCOUNT_{i}_SECRET_KEY')
            account_name = os.getenv(f'ACCOUNT_{i}_NAME')
            
            if api_key and secret_key:
                self.clients[account_name] = AsterDexClient(
                    api_key, secret_key, account_name
                )
                self.logger.info(f"✅ 初始化 {account_name} 客户端")
                
                # 使用第一个有效的客户端作为价格查询客户端
                if self.price_client is None:
                    self.price_client = self.clients[account_name]
            else:
                self.logger.warning(f"⚠️ 无法初始化账户{i}，缺少API密钥")
    
    def get_current_prices(self) -> Dict:
        """获取当前价格"""
        self.logger.info("💰 获取当前代币价格...")
        
        # 从缓存加载价格
        cached_prices = self.cache.load_cached_prices()
        if cached_prices:
            self.logger.info(f"📁 从缓存加载 {len(cached_prices)} 个代币价格")
            return cached_prices
        
        prices = {}
        
        try:
            # 获取所有交易对的最新价格
            self.cache_stats['api_calls_made'] += 1
            all_prices = self.price_client._request('GET', "/api/v1/ticker/price", {})
            
            if isinstance(all_prices, list):
                for price_info in all_prices:
                    symbol = price_info.get('symbol', '')
                    price = float(price_info.get('price', 0))
                    prices[symbol] = price
                
                self.logger.info(f"✅ 获取到 {len(prices)} 个交易对的最新价格")
                
                # 保存到缓存
                self.cache.save_prices_to_cache(prices)
            else:
                self.logger.error(f"❌ 获取价格失败: {all_prices}")
                
        except Exception as e:
            self.logger.error(f"❌ 获取价格时出错: {e}")
        
        return prices
    
    def get_symbol_price(self, symbol: str) -> float:
        """获取指定交易对的价格"""
        if symbol in self.current_prices:
            return self.current_prices[symbol]
        return 0.0
    
    def get_asset_price_in_usdt(self, asset: str) -> float:
        """获取资产对应的USDT价格"""
        if asset == 'USDT':
            return 1.0
        
        # 尝试直接获取交易对价格
        symbol = f"{asset}USDT"
        price = self.get_symbol_price(symbol)
        if price > 0:
            return price
        
        # 如果直接交易对不存在，尝试通过其他方式估算
        # 这里可以添加更多逻辑，比如通过BTC中转等
        self.logger.warning(f"⚠️ 无法获取 {asset} 的USDT价格")
        return 0.0
    
    def get_all_trades_with_pagination(self, client: AsterDexClient, token_symbol: str, from_id: int = None) -> List[Dict]:
        """分页获取所有交易记录（处理1000条限制）"""
        all_trades = []
        current_from_id = from_id
        limit = 1000  # 每次最多获取1000条
        max_attempts = 100  # 最大尝试次数，防止无限循环
        attempt_count = 0
        
        self.logger.info(f"🔄 开始分页获取 {client.account_name} {token_symbol} 交易记录，from_id: {current_from_id}")
        
        while attempt_count < max_attempts:
            attempt_count += 1
            self.cache_stats['api_calls_made'] += 1
            
            try:
                # 准备请求参数
                params = {
                    'symbol': token_symbol,
                    'limit': limit
                }
                
                # 如果有from_id，就加上
                if current_from_id:
                    params['fromId'] = current_from_id
                
                # 获取交易记录
                trades = client._request('GET', "/api/v1/userTrades", params, signed=True)
                
                if not isinstance(trades, list):
                    self.logger.error(f"❌ 获取交易记录失败: {trades}")
                    break
                
                # 过滤指定交易对的记录
                filtered_trades = [trade for trade in trades if trade.get('symbol') == token_symbol]
                
                if not filtered_trades:
                    self.logger.info(f"✅ 没有更多 {token_symbol} 交易记录")
                    break
                
                # 添加到总列表
                all_trades.extend(filtered_trades)
                
                # 获取这批记录中的最大ID
                max_trade_id = max(int(trade['id']) for trade in filtered_trades)
                self.logger.info(f"📄 第{attempt_count}页: 获取到 {len(filtered_trades)} 条记录，最大ID: {max_trade_id}")
                
                # 如果获取的记录数少于limit，说明已经获取完所有记录
                if len(filtered_trades) < limit:
                    self.logger.info(f"✅ 已获取完所有 {token_symbol} 交易记录，共 {len(all_trades)} 条")
                    break
                
                # 设置下一次请求的from_id
                current_from_id = max_trade_id + 1
                
                # 添加延迟，避免请求过于频繁
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"❌ 分页获取交易记录时出错: {e}")
                break
        
        if attempt_count >= max_attempts:
            self.logger.warning(f"⚠️ 达到最大尝试次数 {max_attempts}，停止获取")
        
        return all_trades
    
    def get_trades_with_cache(self, client: AsterDexClient, token_symbol: str) -> List[Dict]:
        """使用缓存获取交易记录（增量更新）"""
        account_name = client.account_name
        
        # 从缓存加载已有交易记录
        cached_trades = self.cache.load_cached_trades(account_name, token_symbol)
        self.cache_stats['cached_trades'] += len(cached_trades)
        
        if cached_trades:
            latest_trade_id = self.cache.get_latest_trade_id(account_name, token_symbol)
            self.logger.info(f"📁 {account_name} {token_symbol}: 缓存中找到 {len(cached_trades)} 笔交易，最新ID: {latest_trade_id}")
            
            # 只获取新交易记录
            new_trades = self.get_all_trades_with_pagination(client, token_symbol, latest_trade_id + 1)
        else:
            latest_trade_id = 1
            self.logger.info(f"📁 {account_name} {token_symbol}: 无缓存数据，开始获取所有历史记录")
            
            # 获取所有历史交易记录
            new_trades = self.get_all_trades_with_pagination(client, token_symbol, latest_trade_id)
        
        self.cache_stats['new_trades'] += len(new_trades)
        
        if new_trades:
            self.logger.info(f"🔄 {account_name} {token_symbol}: 获取到 {len(new_trades)} 笔新交易")
            
            # 合并交易记录
            all_trades = self.cache.merge_trades(cached_trades, new_trades)
            
            # 保存到缓存
            self.cache.save_trades_to_cache(account_name, token_symbol, all_trades)
            
            return all_trades
        else:
            self.logger.info(f"✅ {account_name} {token_symbol}: 无新交易")
            self.cache_stats['api_calls_saved'] += 1
            return cached_trades
    
    def get_account_balance(self, client: AsterDexClient) -> Dict:
        """获取账户余额"""
        account_name = client.account_name
        
        try:
            # 从缓存加载余额
            cached_balance = self.cache.load_cached_balance(account_name)
            
            # 获取最新余额
            self.cache_stats['api_calls_made'] += 1
            account_info = client._request('GET', "/api/v1/account", {}, signed=True)
            
            if not isinstance(account_info, dict) or 'balances' not in account_info:
                self.logger.error(f"❌ 获取账户余额失败: {account_info}")
                return cached_balance
            
            balances = {}
            for balance in account_info['balances']:
                asset = balance['asset']
                free = float(balance.get('free', 0))
                locked = float(balance.get('locked', 0))
                total = free + locked
                
                # 只记录有余额的资产
                if total > 0:
                    balances[asset] = {
                        'free': free,
                        'locked': locked,
                        'total': total
                    }
            
            # 保存到缓存
            self.cache.save_balance_to_cache(account_name, balances)
            
            self.logger.info(f"✅ 获取 {account_name} 余额成功")
            return balances
            
        except Exception as e:
            self.logger.error(f"❌ 获取 {account_name} 余额失败: {e}")
            return cached_balance
    
    def calculate_token_volume_for_account(self, client: AsterDexClient, token_symbol: str) -> Dict:
        """计算指定账户在指定代币上的交易量（使用缓存）"""
        self.logger.info(f"📊 计算 {client.account_name} 的 {token_symbol} 交易量...")
        
        try:
            # 使用缓存获取交易记录
            trades = self.get_trades_with_cache(client, token_symbol)
            
            total_volume_usdt = 0.0
            total_trades = 0
            buy_volume = 0.0
            sell_volume = 0.0
            
            for trade in trades:
                if trade.get('symbol') == token_symbol:
                    quote_qty = float(trade.get('quoteQty', 0))
                    side = trade.get('side', '')
                    
                    total_volume_usdt += quote_qty
                    total_trades += 1
                    
                    if side == 'BUY':
                        buy_volume += quote_qty
                    elif side == 'SELL':
                        sell_volume += quote_qty
            
            stats = {
                'total_volume_usdt': total_volume_usdt,
                'total_trades': total_trades,
                'buy_volume': buy_volume,
                'sell_volume': sell_volume,
                'net_volume': buy_volume - sell_volume
            }
            
            self.logger.info(f"✅ {client.account_name} {token_symbol}: "
                           f"{total_trades}笔交易, {total_volume_usdt:.2f} USDT")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ 计算 {client.account_name} {token_symbol} 交易量失败: {e}")
            return {
                'total_volume_usdt': 0.0,
                'total_trades': 0,
                'buy_volume': 0.0,
                'sell_volume': 0.0,
                'net_volume': 0.0
            }
    
    def load_previous_stats(self) -> Dict:
        """加载之前的统计结果"""
        cached_stats = self.cache.load_cached_stats()
        if cached_stats and 'stats' in cached_stats:
            self.logger.info("📁 找到之前的统计缓存")
            return cached_stats['stats']
        return {}
    
    def get_all_account_balances(self):
        """获取所有账户的余额"""
        self.logger.info("\n💰 开始获取所有账户余额...")
        
        self.balance_stats = {}
        total_balance = {}
        
        for account_name, client in self.clients.items():
            self.logger.info(f"🔄 获取 {account_name} 余额...")
            balances = self.get_account_balance(client)
            self.balance_stats[account_name] = balances
            
            # 累加总余额
            for asset, balance_info in balances.items():
                if asset not in total_balance:
                    total_balance[asset] = 0.0
                total_balance[asset] += balance_info['total']
        
        # 保存总余额
        self.balance_stats['TOTAL'] = total_balance
    
    def calculate_all_volumes(self):
        """计算所有账户所有代币的交易量（基于缓存数据）"""
        self.logger.info("🚀 开始计算所有账户的交易量统计...")
        
        # 重置缓存统计
        self.cache_stats = {
            'cached_trades': 0, 
            'new_trades': 0, 
            'api_calls_made': 0,
            'api_calls_saved': 0
        }
        
        # 初始化统计结果
        self.volume_stats = {}
        
        for token_symbol in self.tokens_to_track:
            self.volume_stats[token_symbol] = {}
            token_total_volume = 0.0
            token_total_trades = 0
            token_total_buy = 0.0
            token_total_sell = 0.0
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"📈 代币: {token_symbol}")
            self.logger.info(f"{'='*60}")
            
            for account_name, client in self.clients.items():
                # 计算当前交易量（基于缓存中的所有交易记录）
                account_stats = self.calculate_token_volume_for_account(client, token_symbol)
                self.volume_stats[token_symbol][account_name] = account_stats
                
                # 累加总统计
                token_total_volume += account_stats['total_volume_usdt']
                token_total_trades += account_stats['total_trades']
                token_total_buy += account_stats['buy_volume']
                token_total_sell += account_stats['sell_volume']
            
            # 保存代币总统计
            self.volume_stats[token_symbol]['TOTAL'] = {
                'total_volume_usdt': token_total_volume,
                'total_trades': token_total_trades,
                'buy_volume': token_total_buy,
                'sell_volume': token_total_sell,
                'net_volume': token_total_buy - token_total_sell
            }
        
        # 保存当前统计结果到缓存
        self.cache.save_stats_to_cache(self.volume_stats)
    
    def print_cache_statistics(self):
        """打印缓存使用统计"""
        self.logger.info("\n💾 缓存使用统计:")
        self.logger.info("-" * 40)
        self.logger.info(f"  缓存交易记录: {self.cache_stats['cached_trades']} 笔")
        self.logger.info(f"  新增交易记录: {self.cache_stats['new_trades']} 笔")
        self.logger.info(f"  API调用次数: {self.cache_stats['api_calls_made']} 次")
        self.logger.info(f"  节省API调用: {self.cache_stats['api_calls_saved']} 次")
        
        total_trades = self.cache_stats['cached_trades'] + self.cache_stats['new_trades']
        if total_trades > 0:
            cache_ratio = (self.cache_stats['cached_trades'] / total_trades) * 100
            self.logger.info(f"  缓存命中率: {cache_ratio:.1f}%")
    
    def print_detailed_statistics(self):
        """打印详细的统计结果"""
        self.logger.info("\n" + "="*80)
        self.logger.info("📊 详细交易量统计结果")
        self.logger.info("="*80)
        
        # 按代币打印
        for token_symbol in self.tokens_to_track:
            self.logger.info(f"\n🎯 代币: {token_symbol}")
            self.logger.info("-" * 50)
            
            token_data = self.volume_stats.get(token_symbol, {})
            total_data = token_data.get('TOTAL', {})
            
            # 打印各账户统计
            for account_name in self.clients.keys():
                if account_name in token_data:
                    stats = token_data[account_name]
                    self.logger.info(f"  {account_name}:")
                    self.logger.info(f"    总交易量: {stats['total_volume_usdt']:>10.2f} USDT")

            # 打印代币总计
            self.logger.info(f"  {'总计':<12}:")
            self.logger.info(f"    总交易量: {total_data.get('total_volume_usdt', 0):>10.2f} USDT")

    def print_combined_account_statistics(self):
        """打印各账户的综合统计（余额和交易量在一起）"""
        self.logger.info("\n" + "="*80)
        self.logger.info("👥 各账户综合统计（余额 + 交易量）")
        self.logger.info("="*80)
        
        # 提取所有跟踪的代币符号（去掉USDT后缀）
        tracked_assets = set()
        for token_symbol in self.tokens_to_track:
            # 假设交易对格式为 XXXUSDT
            if token_symbol.endswith('USDT'):
                asset = token_symbol[:-4]  # 去掉USDT后缀
                tracked_assets.add(asset)
        tracked_assets.add('USDT')  # 总是包含USDT
        
        # 按账户统计总交易量
        account_total_volume = {}
        for account_name in self.clients.keys():
            account_total_volume[account_name] = 0.0
            for token_symbol in self.tokens_to_track:
                token_data = self.volume_stats.get(token_symbol, {})
                if account_name in token_data:
                    account_total_volume[account_name] += token_data[account_name]['total_volume_usdt']
        
        # 计算各账户总资产价值
        account_total_value = {}
        for account_name in self.clients.keys():
            balances = self.balance_stats.get(account_name, {})
            total_value = 0.0
            
            for asset, balance_info in balances.items():
                asset_total = balance_info['total']
                if asset_total > 0:
                    price = self.get_asset_price_in_usdt(asset)
                    asset_value = asset_total * price
                    total_value += asset_value
            
            account_total_value[account_name] = total_value
        
        # 打印各账户的综合信息
        for account_name in self.clients.keys():
            balances = self.balance_stats.get(account_name, {})
            total_volume = account_total_volume.get(account_name, 0)
            total_value = account_total_value.get(account_name, 0)
            
            # 如果账户既没有余额也没有交易量，跳过显示
            if not balances and total_volume == 0:
                continue
                
            self.logger.info(f"\n📊 {account_name}:")
            self.logger.info("-" * 50)
            
            # 显示余额信息
            if balances:
                self.logger.info("  💰 余额:")
                # 显示USDT余额
                usdt_balance = balances.get('USDT', {}).get('total', 0)
                if usdt_balance > 0:
                    self.logger.info(f"    USDT: {usdt_balance:>12.4f} (≈ {usdt_balance:>8.2f} USDT)")
                
                # 显示跟踪的代币余额（如果大于0）
                for asset in tracked_assets:
                    if asset != 'USDT' and asset in balances:
                        balance_info = balances[asset]
                        total_balance = balance_info.get('total', 0)
                        if total_balance > 0:
                            free = balance_info.get('free', 0)
                            locked = balance_info.get('locked', 0)
                            price = self.get_asset_price_in_usdt(asset)
                            asset_value = total_balance * price
                            self.logger.info(f"    {asset}: {total_balance:>12.4f} (≈ {asset_value:>8.2f} USDT)")
                
                # 显示总资产价值
                if total_value > 0:
                    self.logger.info(f"    {'总资产':<8}: {'':>12} (≈ {total_value:>8.2f} USDT)")
            
            # 显示交易量信息
            if total_volume > 0:
                self.logger.info("  📈 交易量统计:")
                self.logger.info(f"    总交易量: {total_volume:>12.2f} USDT")
                
                # 显示各代币交易量详情
                for token_symbol in self.tokens_to_track:
                    token_data = self.volume_stats.get(token_symbol, {})
                    if account_name in token_data:
                        stats = token_data[account_name]
                        token_volume = stats['total_volume_usdt']
                        if token_volume > 0:
                            self.logger.info(f"    {token_symbol}: {token_volume:>12.2f} USDT")
    
    def print_total_balance_statistics(self):
        """打印总余额统计"""
        total_balances = self.balance_stats.get('TOTAL', {})
        if total_balances:
            self.logger.info("\n🌐 总余额统计:")
            self.logger.info("-" * 50)
            
            # 提取所有跟踪的代币符号
            tracked_assets = set()
            for token_symbol in self.tokens_to_track:
                if token_symbol.endswith('USDT'):
                    asset = token_symbol[:-4]
                    tracked_assets.add(asset)
            tracked_assets.add('USDT')
            
            total_portfolio_value = 0.0
            
            # 显示USDT总余额
            total_usdt = total_balances.get('USDT', 0)
            if total_usdt > 0:
                self.logger.info(f"  USDT: {total_usdt:>12.4f} (≈ {total_usdt:>8.2f} USDT)")
                total_portfolio_value += total_usdt
            
            # 显示跟踪的代币总余额（如果大于0）
            for asset in tracked_assets:
                if asset != 'USDT' and asset in total_balances:
                    total_balance = total_balances[asset]
                    if total_balance > 0:
                        price = self.get_asset_price_in_usdt(asset)
                        asset_value = total_balance * price
                        self.logger.info(f"  {asset}: {total_balance:>12.4f} (≈ {asset_value:>8.2f} USDT)")
                        total_portfolio_value += asset_value
            
            # 显示总资产价值
            if total_portfolio_value > 0:
                self.logger.info(f"  {'总资产':<8}: {'':>12} (≈ {total_portfolio_value:>8.2f} USDT)")
    
    def print_summary_statistics(self):
        """打印汇总统计"""
        self.logger.info("\n" + "="*80)
        self.logger.info("📈 汇总统计")
        self.logger.info("="*80)
        
        # 计算全局总计
        global_total_volume = 0.0
        global_total_trades = 0
        global_total_buy = 0.0
        global_total_sell = 0.0
        
        # 按账户统计
        account_totals = {}
        for account_name in self.clients.keys():
            account_totals[account_name] = {
                'volume': 0.0,
                'trades': 0,
                'buy': 0.0,
                'sell': 0.0
            }
        
        for token_symbol in self.tokens_to_track:
            token_data = self.volume_stats.get(token_symbol, {})
            total_data = token_data.get('TOTAL', {})
            
            global_total_volume += total_data.get('total_volume_usdt', 0)
            global_total_trades += total_data.get('total_trades', 0)
            global_total_buy += total_data.get('buy_volume', 0)
            global_total_sell += total_data.get('sell_volume', 0)
            
            # 累加各账户统计
            for account_name in self.clients.keys():
                if account_name in token_data:
                    stats = token_data[account_name]
                    account_totals[account_name]['volume'] += stats['total_volume_usdt']
                    account_totals[account_name]['trades'] += stats['total_trades']
                    account_totals[account_name]['buy'] += stats['buy_volume']
                    account_totals[account_name]['sell'] += stats['sell_volume']
        
        # 打印各账户汇总
        self.logger.info("\n👥 各账户汇总:")
        self.logger.info("-" * 50)
        
        for account_name, totals in account_totals.items():
            if totals['volume'] > 0:  # 只显示有交易量的账户
                self.logger.info(f"  {account_name}:")
                self.logger.info(f"    总交易量:   {totals['volume']:>12.2f} USDT")
        
        # 打印全局总计
        self.logger.info("\n🌐 全局总计:")
        self.logger.info("-" * 50)
        if global_total_trades > 0:
            self.logger.info(f"  总交易笔数: {global_total_trades:>6} 笔")
        if global_total_volume > 0:
            self.logger.info(f"  总交易量:   {global_total_volume:>12.2f} USDT")
        
        # 打印各代币占比
        self.logger.info("\n📊 各代币交易量占比:")
        self.logger.info("-" * 50)
        
        for token_symbol in self.tokens_to_track:
            token_data = self.volume_stats.get(token_symbol, {})
            total_data = token_data.get('TOTAL', {})
            token_volume = total_data.get('total_volume_usdt', 0)
            
            if token_volume > 0:  # 只显示有交易量的代币
                if global_total_volume > 0:
                    percentage = (token_volume / global_total_volume) * 100
                else:
                    percentage = 0
                    
                self.logger.info(f"  {token_symbol:<12}: {token_volume:>12.2f} USDT ({percentage:>5.1f}%)")
    
    def export_to_csv(self, filename: str = None):
        """导出统计结果到CSV文件"""
        if filename is None:
            filename = f"volume_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            import csv
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # 写入表头
                headers = ['代币', '账户', '交易笔数', '总交易量(USDT)', '买入量(USDT)', '卖出量(USDT)', '净交易量(USDT)']
                writer.writerow(headers)
                
                # 写入数据
                for token_symbol in self.tokens_to_track:
                    token_data = self.volume_stats.get(token_symbol, {})
                    
                    # 各账户数据
                    for account_name in self.clients.keys():
                        if account_name in token_data:
                            stats = token_data[account_name]
                            writer.writerow([
                                token_symbol,
                                account_name,
                                stats['total_trades'],
                                f"{stats['total_volume_usdt']:.2f}",
                                f"{stats['buy_volume']:.2f}",
                                f"{stats['sell_volume']:.2f}",
                                f"{stats['net_volume']:.2f}"
                            ])
                    
                    # 代币总计
                    total_data = token_data.get('TOTAL', {})
                    writer.writerow([
                        token_symbol,
                        'TOTAL',
                        total_data.get('total_trades', 0),
                        f"{total_data.get('total_volume_usdt', 0):.2f}",
                        f"{total_data.get('buy_volume', 0):.2f}",
                        f"{total_data.get('sell_volume', 0):.2f}",
                        f"{total_data.get('net_volume', 0):.2f}"
                    ])
                
                writer.writerow([])  # 空行
                
                # 写入余额统计
                writer.writerow(['账户余额统计', '', '', '', '', '', ''])
                for account_name in self.clients.keys():
                    balances = self.balance_stats.get(account_name, {})
                    if balances:
                        writer.writerow([f'{account_name}余额', '', '', '', '', '', ''])
                        for asset, balance_info in balances.items():
                            price = self.get_asset_price_in_usdt(asset)
                            asset_value = balance_info['total'] * price
                            writer.writerow([
                                asset,
                                f"{balance_info['total']:.4f}",
                                f"{balance_info['free']:.4f}",
                                f"{balance_info['locked']:.4f}",
                                f"{price:.4f}",
                                f"{asset_value:.2f}",
                                ''
                            ])
                
                writer.writerow([])  # 空行
                
                # 全局总计
                global_volume = sum(
                    data.get('TOTAL', {}).get('total_volume_usdt', 0) 
                    for data in self.volume_stats.values()
                )
                global_trades = sum(
                    data.get('TOTAL', {}).get('total_trades', 0) 
                    for data in self.volume_stats.values()
                )
                
                writer.writerow(['全局统计', '', '', '', '', '', ''])
                if global_trades > 0:
                    writer.writerow(['总交易笔数', global_trades, '', '', '', '', ''])
                if global_volume > 0:
                    writer.writerow(['总交易量(USDT)', f"{global_volume:.2f}", '', '', '', '', ''])
                
                # 缓存统计
                writer.writerow([])
                writer.writerow(['缓存统计', '', '', '', '', '', ''])
                writer.writerow(['缓存交易记录', self.cache_stats['cached_trades'], '', '', '', '', ''])
                writer.writerow(['新增交易记录', self.cache_stats['new_trades'], '', '', '', '', ''])
                writer.writerow(['API调用次数', self.cache_stats['api_calls_made'], '', '', '', '', ''])
                writer.writerow(['节省API调用', self.cache_stats['api_calls_saved'], '', '', '', '', ''])
            
            self.logger.info(f"✅ 统计结果已导出到: {filename}")
            
        except Exception as e:
            self.logger.error(f"❌ 导出CSV失败: {e}")
    
    def clear_cache(self):
        """清除所有缓存数据"""
        try:
            import shutil
            if os.path.exists(self.cache.cache_dir):
                shutil.rmtree(self.cache.cache_dir)
                os.makedirs(self.cache.cache_dir)
                self.logger.info("✅ 已清除所有缓存数据")
            else:
                self.logger.info("ℹ️ 缓存目录不存在，无需清除")
        except Exception as e:
            self.logger.error(f"❌ 清除缓存失败: {e}")
    
    def run(self, force_refresh: bool = False):
        """运行统计程序"""
        self.logger.info("🚀 开始交易量统计程序（增量更新版）")
        self.logger.info(f"📋 统计账户数量: {len(self.clients)}")
        self.logger.info(f"📋 统计代币数量: {len(self.tokens_to_track)}")
        self.logger.info(f"💾 缓存目录: {self.cache.cache_dir}")
        
        if force_refresh:
            self.logger.info("🔄 强制刷新模式：将清除所有缓存")
            self.clear_cache()
        
        self.logger.info("=" * 60)
        
        try:
            # 获取当前价格
            self.current_prices = self.get_current_prices()
            
            # 获取所有账户余额
            self.get_all_account_balances()
            
            # 计算所有交易量（基于缓存中的所有交易记录）
            self.calculate_all_volumes()
            
            # 打印缓存统计
            self.print_cache_statistics()
            
            # 打印详细统计
            self.print_detailed_statistics()
            
            # 打印各账户综合统计（余额+交易量）
            self.print_combined_account_statistics()
            
            # 打印总余额统计
            self.print_total_balance_statistics()
            
            # 打印汇总统计
            self.print_summary_statistics()
            
            # 导出到CSV
            self.export_to_csv()
            
            self.logger.info("\n✅ 交易量统计完成!")
            
        except Exception as e:
            self.logger.error(f"❌ 统计程序运行出错: {e}")
            raise

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='交易量统计程序')
    parser.add_argument('--clear-cache', action='store_true', help='清除所有缓存数据')
    parser.add_argument('--force-refresh', action='store_true', help='强制刷新所有数据')
    
    args = parser.parse_args()
    
    try:
        stats = VolumeStatistics()
        
        if args.clear_cache:
            stats.clear_cache()
            return
        
        stats.run(force_refresh=args.force_refresh)
        
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")

if __name__ == "__main__":
    main()