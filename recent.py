import os
import json
import logging
from datetime import datetime
from typing import Dict, List
from dotenv import load_dotenv
import sys

def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class RecentTradesViewer:
    """最近交易记录查看器"""
    
    def __init__(self, cache_dir: str = "trade_cache"):
        self.cache_dir = cache_dir
        self.logger = setup_logging()
        self.tokens_to_track = self.load_tokens_config()
        
    def load_tokens_config(self) -> List[str]:
        """加载要统计的代币配置"""
        load_dotenv('account.env')
        tokens_str = os.getenv('TRACK_TOKENS', 'ATUSDT,BTTCUSDT,ASTERUSDT')
        tokens_list = [token.strip() for token in tokens_str.split(',')]
        self.logger.info(f"📋 配置统计的代币: {', '.join(tokens_list)}")
        return tokens_list
    
    def get_account_names(self) -> List[str]:
        """获取所有账户名称"""
        load_dotenv('account.env')
        account_count = int(os.getenv('ACCOUNT_COUNT', 2))
        account_names = []
        
        for i in range(1, account_count + 1):
            account_name = os.getenv(f'ACCOUNT_{i}_NAME')
            if account_name:
                account_names.append(account_name)
        
        return account_names
    
    def get_trades_cache_file(self, account_name: str, symbol: str) -> str:
        """获取交易记录缓存文件路径"""
        safe_symbol = symbol.replace('/', '_')
        return os.path.join(self.cache_dir, f"{account_name}_{safe_symbol}_trades.json")
    
    def load_cached_trades(self, account_name: str, symbol: str) -> List[Dict]:
        """从缓存加载交易记录"""
        cache_file = self.get_trades_cache_file(account_name, symbol)
        
        if not os.path.exists(cache_file):
            return []
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                trades = data.get('trades', [])
                self.logger.debug(f"从缓存加载 {account_name} {symbol}: {len(trades)} 条记录")
                return trades
        except Exception as e:
            self.logger.warning(f"加载交易缓存失败 {account_name} {symbol}: {e}")
            return []
    
    def get_recent_trades_by_account(self, limit: int = 5) -> Dict[str, Dict[str, List]]:
        """获取每个账户每个代币的最近交易记录
        
        Returns:
            Dict: {
                'account1': {
                    'ATUSDT': [trade1, trade2, ...],
                    'BTTCUSDT': [trade1, trade2, ...]
                },
                ...
            }
        """
        account_names = self.get_account_names()
        all_recent_trades = {}
        
        self.logger.info(f"🔍 开始分析 {len(account_names)} 个账户的交易记录...")
        
        for account_name in account_names:
            account_trades = {}
            has_trades = False
            
            for token_symbol in self.tokens_to_track:
                # 加载该账户该代币的所有交易记录
                trades = self.load_cached_trades(account_name, token_symbol)
                
                if trades:
                    # 按交易ID倒序排列（假设ID越大越新）
                    try:
                        sorted_trades = sorted(
                            trades, 
                            key=lambda x: int(x.get('id', 0)), 
                            reverse=True
                        )
                        # 取前limit条
                        recent_trades = sorted_trades[:limit]
                        account_trades[token_symbol] = recent_trades
                        has_trades = True
                        
                        self.logger.debug(f"{account_name} {token_symbol}: 找到 {len(recent_trades)} 条最近交易")
                    except Exception as e:
                        self.logger.warning(f"处理 {account_name} {token_symbol} 交易记录时出错: {e}")
            
            if has_trades:
                all_recent_trades[account_name] = account_trades
        
        return all_recent_trades
    
    def format_trade_time(self, trade: Dict) -> str:
        """格式化交易时间"""
        if 'time' in trade:
            try:
                trade_time = datetime.fromtimestamp(trade['time'] / 1000)
                return trade_time.strftime('%m-%d %H:%M:%S')
            except:
                pass
        return "Unknown"
    
    def format_trade_side(self, side: str) -> str:
        """格式化交易方向"""
        if side == 'BUY':
            return "🟢 BUY"
        elif side == 'SELL':
            return "🔴 SELL"
        else:
            return f"❓ {side}"
    
    def print_recent_trades_table(self, recent_trades: Dict[str, Dict[str, List]], limit: int = 5):
        """以表格形式打印最近交易记录"""
        print(f"\n{'='*100}")
        print(f"📊 各账户最近 {limit} 条交易记录")
        print(f"{'='*100}")
        
        if not recent_trades:
            print("❌ 未找到任何交易记录")
            print("请先运行主统计程序生成缓存数据")
            return
        
        for account_name, token_trades in recent_trades.items():
            print(f"\n👤 账户: {account_name}")
            print("-" * 100)
            
            if not token_trades:
                print("   暂无交易记录")
                continue
            
            for token_symbol, trades in token_trades.items():
                if trades:
                    print(f"\n  💰 代币: {token_symbol}")
                    print("  " + "-" * 90)
                    
                    # 表头
                    header = f"  {'时间':<18} {'方向':<8} {'数量':<12} {'价格':<12} {'金额(USDT)':<12} {'交易ID':<10}"
                    print(header)
                    print("  " + "-" * 90)
                    
                    # 交易记录
                    for trade in trades:
                        trade_id = trade.get('id', 'N/A')
                        side = trade.get('side', 'UNKNOWN')
                        quantity = float(trade.get('qty', 0))
                        price = float(trade.get('price', 0))
                        quote_qty = float(trade.get('quoteQty', 0))
                        
                        time_str = self.format_trade_time(trade)
                        side_str = self.format_trade_side(side)
                        quantity_str = f"{quantity:.4f}"
                        price_str = f"{price:.6f}"
                        amount_str = f"{quote_qty:.2f}"
                        
                        trade_line = f"  {time_str:<18} {side_str:<8} {quantity_str:<12} {price_str:<12} {amount_str:<12} {trade_id:<10}"
                        print(trade_line)
    
    def print_compact_view(self, recent_trades: Dict[str, Dict[str, List]], limit: int = 5):
        """简洁视图 - 每个账户一行汇总"""
        print(f"\n{'='*80}")
        print(f"📋 交易记录汇总 (最近{limit}条/代币)")
        print(f"{'='*80}")
        
        if not recent_trades:
            print("❌ 未找到任何交易记录")
            return
        
        for account_name, token_trades in recent_trades.items():
            print(f"\n👤 {account_name}:")
            
            if not token_trades:
                print("   暂无交易记录")
                continue
            
            for token_symbol, trades in token_trades.items():
                if trades:
                    # 统计买卖数量
                    buy_count = sum(1 for trade in trades if trade.get('side') == 'BUY')
                    sell_count = sum(1 for trade in trades if trade.get('side') == 'SELL')
                    total_volume = sum(float(trade.get('quoteQty', 0)) for trade in trades)
                    
                    latest_trade = trades[0]  # 最新的交易
                    latest_time = self.format_trade_time(latest_trade)
                    latest_side = "↑" if latest_trade.get('side') == 'BUY' else "↓"
                    
                    print(f"  {token_symbol:<12} {latest_side} {latest_time} | "
                          f"买:{buy_count} 卖:{sell_count} | 总金额:{total_volume:.0f} USDT")
    
    def print_token_summary(self, recent_trades: Dict[str, Dict[str, List]]):
        """按代币汇总视图"""
        print(f"\n{'='*80}")
        print(f"🎯 按代币汇总")
        print(f"{'='*80}")
        
        if not recent_trades:
            return
        
        # 按代币组织数据
        token_data = {}
        
        for account_name, token_trades in recent_trades.items():
            for token_symbol, trades in token_trades.items():
                if token_symbol not in token_data:
                    token_data[token_symbol] = []
                
                for trade in trades:
                    trade_copy = trade.copy()
                    trade_copy['account'] = account_name
                    token_data[token_symbol].append(trade_copy)
        
        # 打印每个代币的交易
        for token_symbol, all_trades in token_data.items():
            # 按时间排序
            sorted_trades = sorted(
                all_trades,
                key=lambda x: x.get('time', 0),
                reverse=True
            )[:10]  # 显示最近10条
            
            print(f"\n💰 {token_symbol} (最近{len(sorted_trades)}条):")
            print("-" * 80)
            
            for trade in sorted_trades:
                account = trade.get('account', 'Unknown')
                trade_id = trade.get('id', 'N/A')
                side = "↑" if trade.get('side') == 'BUY' else "↓"
                quantity = float(trade.get('qty', 0))
                price = float(trade.get('price', 0))
                quote_qty = float(trade.get('quoteQty', 0))
                time_str = self.format_trade_time(trade)
                
                print(f"  {time_str} {account:<10} {side} {quantity:>8.2f} @ {price:<8.4f} "
                      f"(≈{quote_qty:>8.2f} USDT) ID:{trade_id}")
    
    def run(self, limit: int = 5, view_type: str = "detailed"):
        """运行最近交易记录查看器
        
        Args:
            limit: 每个代币显示的交易记录数量
            view_type: 显示类型 - 'detailed', 'compact', 'summary', 'all'
        """
        self.logger.info("🚀 启动最近交易记录查看器")
        self.logger.info(f"📁 缓存目录: {self.cache_dir}")
        self.logger.info(f"📋 跟踪代币: {', '.join(self.tokens_to_track)}")
        self.logger.info(f"🔢 显示数量: 最近{limit}条/代币")
        
        # 检查缓存目录是否存在
        if not os.path.exists(self.cache_dir):
            self.logger.error(f"❌ 缓存目录不存在: {self.cache_dir}")
            self.logger.info("请先运行主统计程序生成缓存数据")
            return
        
        # 获取最近交易记录
        recent_trades = self.get_recent_trades_by_account(limit)
        
        if not recent_trades:
            self.logger.error("❌ 未找到任何交易记录")
            self.logger.info("可能的原因:")
            self.logger.info("1. 缓存目录为空")
            self.logger.info("2. 还没有进行过交易")
            self.logger.info("3. 账户配置不正确")
            return
        
        # 统计信息
        total_accounts = len(recent_trades)
        total_tokens = sum(len(tokens) for tokens in recent_trades.values())
        total_trades = sum(len(trades) for token_trades in recent_trades.values() 
                          for trades in token_trades.values())
        
        self.logger.info(f"✅ 找到 {total_trades} 条交易记录 "
                        f"({total_accounts}个账户, {total_tokens}个代币)")
        
        # 根据视图类型显示
        if view_type in ["detailed", "all"]:
            self.print_recent_trades_table(recent_trades, limit)
        
        if view_type in ["compact", "all"]:
            self.print_compact_view(recent_trades, limit)
        
        if view_type in ["summary", "all"]:
            self.print_token_summary(recent_trades)

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='最近交易记录查看器')
    parser.add_argument('--limit', type=int, default=5, 
                       help='每个代币显示的交易记录数量，默认5条')
    parser.add_argument('--view', type=str, default='detailed',
                       choices=['detailed', 'compact', 'summary', 'all'],
                       help='显示类型: detailed(详细表格), compact(简洁视图), summary(代币汇总), all(全部显示)')
    parser.add_argument('--cache-dir', type=str, default='trade_cache',
                       help='缓存目录路径，默认trade_cache')
    
    args = parser.parse_args()
    
    try:
        viewer = RecentTradesViewer(cache_dir=args.cache_dir)
        viewer.run(limit=args.limit, view_type=args.view)
        
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")

if __name__ == "__main__":
    main()