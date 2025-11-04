import os
from dotenv import load_dotenv
import logging
from typing import Dict, List
from market_maker import AsterDexClient
import sys
from datetime import datetime

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

class VolumeStatistics:
    """交易量统计程序"""
    
    def __init__(self):
        # 加载环境变量
        load_dotenv('account.env')
        
        # 设置日志
        self.logger = setup_logging()
        
        # 初始化客户端
        self.clients = {}
        self.init_clients()
        
        # 配置要统计的代币
        self.tokens_to_track = self.load_tokens_config()
        
        # 统计结果
        self.volume_stats = {}
    
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
            else:
                self.logger.warning(f"⚠️ 无法初始化 {account_name}，缺少API密钥")
    
    def calculate_token_volume_for_account(self, client: AsterDexClient, token_symbol: str) -> Dict:
        """计算指定账户在指定代币上的交易量"""
        self.logger.info(f"📊 计算 {client.account_name} 的 {token_symbol} 交易量...")
        
        try:
            # 获取所有历史交易
            trades = client.get_all_user_trades(symbol=token_symbol)
            
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
                'net_volume': buy_volume - sell_volume  # 正数表示净买入，负数表示净卖出
            }
            
            self.logger.info(f"✅ {client.account_name} {token_symbol}: "
                           f"{total_trades}笔交易, {total_volume_usdt:.2f} USDT, "
                           f"买入{buy_volume:.2f}, 卖出{sell_volume:.2f}")
            
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
    
    def calculate_all_volumes(self):
        """计算所有账户所有代币的交易量"""
        self.logger.info("🚀 开始计算所有账户的交易量统计...")
        
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
                stats = self.calculate_token_volume_for_account(client, token_symbol)
                self.volume_stats[token_symbol][account_name] = stats
                
                # 累加总统计
                token_total_volume += stats['total_volume_usdt']
                token_total_trades += stats['total_trades']
                token_total_buy += stats['buy_volume']
                token_total_sell += stats['sell_volume']
            
            # 保存代币总统计
            self.volume_stats[token_symbol]['TOTAL'] = {
                'total_volume_usdt': token_total_volume,
                'total_trades': token_total_trades,
                'buy_volume': token_total_buy,
                'sell_volume': token_total_sell,
                'net_volume': token_total_buy - token_total_sell
            }
    
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
                    # self.logger.info(f"    交易笔数: {stats['total_trades']:>6} 笔")
                    self.logger.info(f"    总交易量: {stats['total_volume_usdt']:>10.2f} USDT")

            # 打印代币总计
            self.logger.info(f"  {'总计':<12}:")
            # self.logger.info(f"    交易笔数: {total_data.get('total_trades', 0):>6} 笔")
            self.logger.info(f"    总交易量: {total_data.get('total_volume_usdt', 0):>10.2f} USDT")


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
            self.logger.info(f"  {account_name}:")
            # self.logger.info(f"    总交易笔数: {totals['trades']:>6} 笔")
            self.logger.info(f"    总交易量:   {totals['volume']:>12.2f} USDT")
            # self.logger.info(f"    总买入量:   {totals['buy']:>12.2f} USDT")
            # self.logger.info(f"    总卖出量:   {totals['sell']:>12.2f} USDT")
            # self.logger.info(f"    净交易量:   {totals['buy'] - totals['sell']:>12.2f} USDT")
        
        # 打印全局总计
        self.logger.info("\n🌐 全局总计:")
        self.logger.info("-" * 50)
        self.logger.info(f"  总交易笔数: {global_total_trades:>6} 笔")
        self.logger.info(f"  总交易量:   {global_total_volume:>12.2f} USDT")
        self.logger.info(f"  总买入量:   {global_total_buy:>12.2f} USDT")
        self.logger.info(f"  总卖出量:   {global_total_sell:>12.2f} USDT")
        self.logger.info(f"  净交易量:   {global_total_buy - global_total_sell:>12.2f} USDT")
        
        # 打印各代币占比
        self.logger.info("\n📊 各代币交易量占比:")
        self.logger.info("-" * 50)
        
        for token_symbol in self.tokens_to_track:
            token_data = self.volume_stats.get(token_symbol, {})
            total_data = token_data.get('TOTAL', {})
            token_volume = total_data.get('total_volume_usdt', 0)
            
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
                writer.writerow(['总交易笔数', global_trades, '', '', '', '', ''])
                writer.writerow(['总交易量(USDT)', f"{global_volume:.2f}", '', '', '', '', ''])
            
            self.logger.info(f"✅ 统计结果已导出到: {filename}")
            
        except ImportError:
            self.logger.error("❌ 无法导出CSV，请安装csv模块")
        except Exception as e:
            self.logger.error(f"❌ 导出CSV失败: {e}")
    
    def run(self):
        """运行统计程序"""
        self.logger.info("🚀 开始交易量统计程序")
        self.logger.info(f"📋 统计账户数量: {len(self.clients)}")
        self.logger.info(f"📋 统计代币数量: {len(self.tokens_to_track)}")
        self.logger.info("=" * 60)
        
        try:
            # 计算所有交易量
            self.calculate_all_volumes()
            
            # 打印详细统计
            self.print_detailed_statistics()
            
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
    try:
        stats = VolumeStatistics()
        stats.run()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")

if __name__ == "__main__":
    main()