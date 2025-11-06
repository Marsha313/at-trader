import pandas as pd
import argparse
import sys
import os
from datetime import datetime
import logging
import numpy as np
import requests
import time
import json
import re
import glob

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

class TradingLossCalculator:
    """交易损耗计算器"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.df1 = None
        self.df2 = None
        self.current_prices = {}  # 存储当前价格
        
    def safe_float_convert(self, value, default=0.0):
        """安全转换为浮点数，处理NaN和空值"""
        if pd.isna(value) or value == '' or value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def find_latest_volume_stats_files(self):
        """自动查找最新的两个volume_stats文件"""
        try:
            # 查找所有volume_stats开头的CSV文件
            pattern = "volume_stats_*.csv"
            files = glob.glob(pattern)
            
            if not files:
                self.logger.error("❌ 未找到任何volume_stats开头的CSV文件")
                return None, None
            
            # 提取文件名中的时间信息并排序
            file_times = []
            for file in files:
                # 从文件名中提取时间戳，格式：volume_stats_YYYYMMDD_HHMMSS.csv
                match = re.search(r'volume_stats_(\d{8}_\d{6})\.csv', file)
                if match:
                    time_str = match.group(1)
                    try:
                        file_time = datetime.strptime(time_str, '%Y%m%d_%H%M%S')
                        file_times.append((file, file_time))
                    except ValueError:
                        self.logger.warning(f"⚠️ 无法解析文件名中的时间戳: {file}")
                        continue
            
            if len(file_times) < 2:
                self.logger.error(f"❌ 找到的文件数量不足2个，当前找到 {len(file_times)} 个有效文件")
                return None, None
            
            # 按时间戳排序，最新的在前面
            file_times.sort(key=lambda x: x[1], reverse=True)
            
            # 获取最新的两个文件
            latest_file = file_times[0][0]
            second_latest_file = file_times[1][0]
            
            self.logger.info(f"📁 自动找到的最新文件: {latest_file}")
            self.logger.info(f"📁 自动找到的次新文件: {second_latest_file}")
            
            return second_latest_file, latest_file
            
        except Exception as e:
            self.logger.error(f"❌ 自动查找文件失败: {e}")
            return None, None
    
    def get_current_prices(self):
        """获取当前所有代币的USDT价格"""
        self.logger.info("💰 获取当前代币价格...")
        
        try:
            # 使用Aster API获取所有交易对价格
            url = "https://sapi.asterdex.com/api/v1/ticker/price"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                prices = {}
                
                if isinstance(data, list):
                    for item in data:
                        symbol = item.get('symbol', '')
                        price = self.safe_float_convert(item.get('price', 0))
                        if symbol and price > 0:
                            prices[symbol] = price
                
                self.logger.info(f"✅ 获取到 {len(prices)} 个交易对的最新价格")
                return prices
            else:
                self.logger.error(f"❌ 获取价格API失败: {response.status_code}")
                return {}
                
        except Exception as e:
            self.logger.error(f"❌ 获取价格失败: {e}")
            return {}
    
    def get_asset_price_in_usdt(self, asset: str) -> float:
        """获取资产对应的USDT价格"""
        if asset == 'USDT':
            return 1.0
        
        # 尝试直接获取交易对价格
        symbol = f"{asset}USDT"
        if symbol in self.current_prices:
            return self.current_prices[symbol]
        
        # 如果直接交易对不存在，尝试其他可能的形式
        # 比如有些交易对可能是 USDT在前
        for price_symbol, price in self.current_prices.items():
            if price_symbol.endswith(asset) and price_symbol.startswith('USDT'):
                return 1.0 / price if price > 0 else 0.0
        
        self.logger.warning(f"⚠️ 无法获取 {asset} 的USDT价格")
        return 0.0
    
    def load_csv_files(self, file1: str, file2: str):
        """加载两个CSV文件"""
        try:
            self.logger.info(f"📁 加载文件1: {file1}")
            self.df1 = pd.read_csv(file1)
            
            self.logger.info(f"📁 加载文件2: {file2}")
            self.df2 = pd.read_csv(file2)
            
            self.logger.info("✅ CSV文件加载成功")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 加载CSV文件失败: {e}")
            return False
    
    def extract_account_balances(self, df: pd.DataFrame) -> dict:
        """从DataFrame中提取账户余额信息（只提取数量，不提取价值）"""
        account_balances = {}
        
        try:
            # 查找余额统计开始的位置
            balance_start_idx = None
            for idx, row in df.iterrows():
                if '账户余额统计' in str(row.iloc[0]):
                    balance_start_idx = idx
                    break
            
            if balance_start_idx is None:
                self.logger.warning("⚠️ 未找到余额统计信息")
                return account_balances
            
            current_account = None
            current_balances = {}
            
            for idx in range(balance_start_idx + 1, len(df)):
                row = df.iloc[idx]
                first_col = str(row.iloc[0])
                
                # 跳过空行
                if pd.isna(first_col) or first_col == '':
                    continue
                
                # 检查是否是新的账户余额部分
                if '余额' in first_col:
                    # 保存前一个账户的余额
                    if current_account and current_balances:
                        account_balances[current_account] = current_balances
                    
                    # 开始新的账户
                    current_account = first_col.replace('余额', '').strip()
                    current_balances = {}
                
                # 处理余额数据行
                elif current_account and len(row) >= 6:
                    asset = str(row.iloc[0]).strip()
                    if asset and asset not in ['全局统计', '缓存统计']:
                        try:
                            total_balance = self.safe_float_convert(row.iloc[1])
                            
                            # 只有当数值有效时才记录
                            if total_balance > 0:
                                current_balances[asset] = total_balance
                        except (ValueError, TypeError):
                            continue
            
            # 保存最后一个账户的余额
            if current_account and current_balances:
                account_balances[current_account] = current_balances
            
            self.logger.info(f"📊 提取到 {len(account_balances)} 个账户的余额信息")
            return account_balances
            
        except Exception as e:
            self.logger.error(f"❌ 提取余额信息失败: {e}")
            return {}
    
    def calculate_portfolio_value(self, balances: dict) -> float:
        """使用当前价格计算投资组合价值"""
        total_value = 0.0
        for asset, quantity in balances.items():
            price = self.get_asset_price_in_usdt(asset)
            asset_value = quantity * price
            if not pd.isna(asset_value) and asset_value > 0:
                total_value += asset_value
        return total_value
    
    def extract_trading_volume(self, df: pd.DataFrame) -> dict:
        """从DataFrame中提取交易量信息"""
        account_volume = {}
        
        try:
            # 处理交易量数据（在余额统计之前的部分）
            for idx, row in df.iterrows():
                first_col = str(row.iloc[0])
                
                # 检查是否到达余额统计部分
                if '账户余额统计' in first_col:
                    break
                
                # 处理交易量数据行
                if (not pd.isna(row.iloc[1]) and 
                    str(row.iloc[1]) not in ['TOTAL', ''] and 
                    '代币' not in first_col):
                    
                    account_name = str(row.iloc[1])
                    symbol = str(row.iloc[0])
                    
                    if account_name not in account_volume:
                        account_volume[account_name] = {}
                    
                    try:
                        volume = self.safe_float_convert(row.iloc[3])
                        if volume > 0:  # 只记录有效交易量
                            account_volume[account_name][symbol] = volume
                    except (ValueError, TypeError):
                        continue
            
            self.logger.info(f"📈 提取到 {len(account_volume)} 个账户的交易量信息")
            return account_volume
            
        except Exception as e:
            self.logger.error(f"❌ 提取交易量信息失败: {e}")
            return {}
    
    def calculate_total_trading_volume(self, volumes: dict) -> float:
        """计算总交易量"""
        total_volume = 0.0
        for volume in volumes.values():
            if not pd.isna(volume) and volume > 0:
                total_volume += volume
        return total_volume
    
    def calculate_loss_analysis(self):
        """计算交易损耗分析"""
        try:
            self.logger.info("\n" + "="*80)
            self.logger.info("📊 交易损耗分析计算")
            self.logger.info("="*80)
            
            # 首先获取当前价格
            self.current_prices = self.get_current_prices()
            if not self.current_prices:
                self.logger.error("❌ 无法获取当前价格，无法进行计算")
                return
            
            # 提取两个时间点的数据
            balances1 = self.extract_account_balances(self.df1)
            balances2 = self.extract_account_balances(self.df2)
            volumes1 = self.extract_trading_volume(self.df1)
            volumes2 = self.extract_trading_volume(self.df2)
            
            if not balances1 or not balances2:
                self.logger.error("❌ 无法提取足够的余额数据进行计算")
                return
            
            # 分析每个账户
            account_analysis = {}
            valid_accounts = []  # 记录有交易活动的账户（用于损耗率计算）
            all_valid_accounts = []  # 记录所有有效账户（包括无交易活动的）
            
            # 获取所有账户名称（两个文件的并集）
            all_accounts = set(balances1.keys()) | set(balances2.keys())
            
            for account in all_accounts:
                self.logger.info(f"\n🔍 分析账户: {account}")
                
                # 使用当前价格计算两个时间点的投资组合价值
                portfolio_value1 = self.calculate_portfolio_value(balances1.get(account, {}))
                portfolio_value2 = self.calculate_portfolio_value(balances2.get(account, {}))
                
                # 检查数据有效性
                if pd.isna(portfolio_value1) or pd.isna(portfolio_value2):
                    self.logger.warning(f"   ⚠️ 账户 {account} 的投资组合价值包含NaN，跳过计算")
                    continue
                
                portfolio_change = portfolio_value2 - portfolio_value1
                
                # 计算交易量变化
                total_volume1 = self.calculate_total_trading_volume(volumes1.get(account, {}))
                total_volume2 = self.calculate_total_trading_volume(volumes2.get(account, {}))
                
                if pd.isna(total_volume1) or pd.isna(total_volume2):
                    self.logger.warning(f"   ⚠️ 账户 {account} 的交易量包含NaN，跳过计算")
                    continue
                
                volume_change = total_volume2 - total_volume1
                
                # 计算损耗和损耗率
                loss = -portfolio_change  # 负的价值变化表示损耗
                
                # 只有交易量变化大于0时才计算损耗率
                if volume_change > 0:
                    loss_rate = (loss / volume_change * 100)
                else:
                    loss_rate = None  # 交易量变化为0，不计算损耗率
                
                account_analysis[account] = {
                    'portfolio_value1': portfolio_value1,
                    'portfolio_value2': portfolio_value2,
                    'portfolio_change': portfolio_change,
                    'total_volume1': total_volume1,
                    'total_volume2': total_volume2,
                    'volume_change': volume_change,
                    'loss': loss,
                    'loss_rate': loss_rate,
                    'has_trading_activity': volume_change > 0,  # 标记是否有交易活动
                    'balances1': balances1.get(account, {}),
                    'balances2': balances2.get(account, {})
                }
                
                all_valid_accounts.append(account)
                if volume_change > 0:
                    valid_accounts.append(account)
                
                self.logger.info(f"   投资组合价值: {portfolio_value1:.2f} → {portfolio_value2:.2f} USDT")
                self.logger.info(f"   价值变化: {portfolio_change:+.2f} USDT")
                self.logger.info(f"   总交易量: {total_volume1:.2f} → {total_volume2:.2f} USDT")
                self.logger.info(f"   交易量变化: {volume_change:.2f} USDT")
                self.logger.info(f"   交易损耗: {loss:.2f} USDT")
                
                if volume_change > 0:
                    self.logger.info(f"   损耗率: {loss_rate:.4f}%")
                else:
                    self.logger.info("   损耗率: 无交易活动，不计算损耗率")
                
                # 显示详细的资产变化
                self.logger.info("   资产明细:")
                all_assets = set(balances1.get(account, {}).keys()) | set(balances2.get(account, {}).keys())
                for asset in all_assets:
                    qty1 = balances1.get(account, {}).get(asset, 0)
                    qty2 = balances2.get(account, {}).get(asset, 0)
                    price = self.get_asset_price_in_usdt(asset)
                    if qty1 != qty2 or (qty1 > 0 and qty2 > 0):
                        self.logger.info(f"     {asset}: {qty1:.4f} → {qty2:.4f} (价格: {price:.4f} USDT)")
            
            if not all_valid_accounts:
                self.logger.error("❌ 没有找到有效的账户数据进行计算")
                return
            
            # 计算总计（使用所有有效账户计算投资组合价值，但只使用有交易活动的账户计算损耗率）
            total_portfolio_value1 = 0.0
            total_portfolio_value2 = 0.0
            total_volume_change = 0.0
            total_loss = 0.0
            
            for account in all_valid_accounts:
                data = account_analysis[account]
                total_portfolio_value1 += data['portfolio_value1']
                total_portfolio_value2 += data['portfolio_value2']
                if data['has_trading_activity']:  # 只有有交易活动的账户才计入损耗统计
                    total_volume_change += data['volume_change']
                    total_loss += data['loss']
            
            total_portfolio_change = total_portfolio_value2 - total_portfolio_value1
            total_loss_rate = (total_loss / total_volume_change * 100) if total_volume_change != 0 else 0
            
            # 打印详细报告
            self.print_detailed_report(account_analysis, all_valid_accounts, valid_accounts, total_loss, total_loss_rate)
            
            # 导出结果到CSV
            self.export_loss_analysis(account_analysis, all_valid_accounts, valid_accounts, total_loss, total_loss_rate)
            
        except Exception as e:
            self.logger.error(f"❌ 计算交易损耗分析失败: {e}")
            import traceback
            traceback.print_exc()
    
    def print_detailed_report(self, account_analysis: dict, all_valid_accounts: list, valid_accounts: list, total_loss: float, total_loss_rate: float):
        """打印详细报告"""
        self.logger.info("\n" + "="*80)
        self.logger.info("📈 交易损耗详细报告")
        self.logger.info("="*80)
        
        if not all_valid_accounts:
            self.logger.info("⚠️ 没有有效的账户数据")
            return
        
        # 按损耗率排序（有交易活动的账户在前，无交易活动的在后）
        trading_accounts = [(acc, account_analysis[acc]) for acc in valid_accounts]
        non_trading_accounts = [(acc, account_analysis[acc]) for acc in all_valid_accounts if acc not in valid_accounts]
        
        # 有交易活动的账户按损耗率排序
        sorted_trading_accounts = sorted(trading_accounts, key=lambda x: x[1]['loss_rate'], reverse=True)
        # 无交易活动的账户按账户名称排序
        sorted_non_trading_accounts = sorted(non_trading_accounts, key=lambda x: x[0])
        
        sorted_accounts = sorted_trading_accounts + sorted_non_trading_accounts
        
        self.logger.info(f"\n👥 各账户情况 (共 {len(all_valid_accounts)} 个有效账户，其中 {len(valid_accounts)} 个有交易活动):")
        self.logger.info("-" * 130)
        self.logger.info(f"{'账户':<15} {'初始价值':>12} {'最终价值':>12} {'价值变化':>12} {'交易量变化':>12} {'交易损耗':>12} {'损耗率':>10} {'状态':>8}")
        self.logger.info("-" * 130)
        
        for account, data in sorted_accounts:
            if data['has_trading_activity']:
                status = "交易中"
                loss_rate_display = f"{data['loss_rate']:>9.3f}%"
            else:
                status = "无交易"
                loss_rate_display = "   -   "
            
            self.logger.info(
                f"{account:<15} "
                f"{data['portfolio_value1']:>12.2f} "
                f"{data['portfolio_value2']:>12.2f} "
                f"{data['portfolio_change']:>+12.2f} "
                f"{data['volume_change']:>12.2f} "
                f"{data['loss']:>12.2f} "
                f"{loss_rate_display} "
                f"{status:>8}"
            )
        
        self.logger.info("-" * 130)
        
        # 计算总计
        total_portfolio_value1 = sum(account_analysis[acc]['portfolio_value1'] for acc in all_valid_accounts)
        total_portfolio_value2 = sum(account_analysis[acc]['portfolio_value2'] for acc in all_valid_accounts)
        total_volume_change = sum(account_analysis[acc]['volume_change'] for acc in valid_accounts)  # 只计算有交易活动的
        
        self.logger.info(f"{'总计':<15} "
                        f"{total_portfolio_value1:>12.2f} "
                        f"{total_portfolio_value2:>12.2f} "
                        f"{(total_portfolio_value2 - total_portfolio_value1):>+12.2f} "
                        f"{total_volume_change:>12.2f} "
                        f"{total_loss:>12.2f} "
                        f"{total_loss_rate:>9.3f}% "
                        f"{'':>8}")
        
        # 打印分析总结
        self.logger.info("\n📋 分析总结:")
        self.logger.info("-" * 50)
        
        if all_valid_accounts:
            self.logger.info(f"总账户数量: {len(all_valid_accounts)}")
            self.logger.info(f"有交易活动账户: {len(valid_accounts)}")
            self.logger.info(f"无交易活动账户: {len(all_valid_accounts) - len(valid_accounts)}")
            
            if valid_accounts:
                avg_loss_rate = sum(account_analysis[acc]['loss_rate'] for acc in valid_accounts) / len(valid_accounts)
                max_loss_account = max(valid_accounts, key=lambda x: account_analysis[x]['loss_rate'])
                min_loss_account = min(valid_accounts, key=lambda x: account_analysis[x]['loss_rate'])
                
                self.logger.info(f"平均损耗率: {avg_loss_rate:.3f}%")
                self.logger.info(f"最高损耗率账户: {max_loss_account} ({account_analysis[max_loss_account]['loss_rate']:.3f}%)")
                self.logger.info(f"最低损耗率账户: {min_loss_account} ({account_analysis[min_loss_account]['loss_rate']:.3f}%)")
                self.logger.info(f"总交易损耗: {total_loss:.2f} USDT")
                self.logger.info(f"总损耗率: {total_loss_rate:.3f}%")
            else:
                self.logger.info("⚠️ 没有发现活跃交易账户")
            
            self.logger.info(f"总投资组合价值变化: {total_portfolio_value2 - total_portfolio_value1:+.2f} USDT")
            self.logger.info(f"总交易量变化: {total_volume_change:.2f} USDT")
            self.logger.info(f"使用统一价格计算时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            self.logger.info("⚠️ 没有发现有效的交易账户")
    
    def export_loss_analysis(self, account_analysis: dict, all_valid_accounts: list, valid_accounts: list, total_loss: float, total_loss_rate: float):
        """导出损耗分析结果到CSV"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"trading_loss_analysis_{timestamp}.csv"
            
            with open(filename, 'w', encoding='utf-8') as f:
                # 写入表头
                f.write("账户,初始投资组合价值(USDT),最终投资组合价值(USDT),价值变化(USDT),交易量变化(USDT),交易损耗(USDT),损耗率(%),状态\n")
                
                # 写入各账户数据
                for account in all_valid_accounts:
                    data = account_analysis[account]
                    status = "有交易" if data['has_trading_activity'] else "无交易"
                    loss_rate = f"{data['loss_rate']:.5f}" if data['has_trading_activity'] else ""
                    
                    f.write(
                        f"{account},"
                        f"{data['portfolio_value1']:.2f},"
                        f"{data['portfolio_value2']:.2f},"
                        f"{data['portfolio_change']:.2f},"
                        f"{data['volume_change']:.2f},"
                        f"{data['loss']:.2f},"
                        f"{loss_rate},"
                        f"{status}\n"
                    )
                
                # 写入总计
                total_portfolio_value1 = sum(account_analysis[acc]['portfolio_value1'] for acc in all_valid_accounts)
                total_portfolio_value2 = sum(account_analysis[acc]['portfolio_value2'] for acc in all_valid_accounts)
                total_volume_change = sum(account_analysis[acc]['volume_change'] for acc in valid_accounts)
                
                f.write(
                    f"总计,"
                    f"{total_portfolio_value1:.2f},"
                    f"{total_portfolio_value2:.2f},"
                    f"{(total_portfolio_value2 - total_portfolio_value1):.2f},"
                    f"{total_volume_change:.2f},"
                    f"{total_loss:.2f},"
                    f"{total_loss_rate:.5f},"
                    f"有交易\n"
                )
                
                # 写入价格信息
                f.write("\n使用的价格信息:\n")
                f.write("代币,价格(USDT)\n")
                for asset in set().union(*[account_analysis[acc]['balances1'].keys() for acc in all_valid_accounts],
                                       *[account_analysis[acc]['balances2'].keys() for acc in all_valid_accounts]):
                    if asset != 'USDT':
                        price = self.get_asset_price_in_usdt(asset)
                        if price > 0:
                            f.write(f"{asset},{price:.6f}\n")
            
            self.logger.info(f"✅ 损耗分析结果已导出到: {filename}")
            
        except Exception as e:
            self.logger.error(f"❌ 导出损耗分析结果失败: {e}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='交易损耗分析工具')
    parser.add_argument('file1', nargs='?', help='第一个volume_stats CSV文件（较早时间点）')
    parser.add_argument('file2', nargs='?', help='第二个volume_stats CSV文件（较晚时间点）')
    
    args = parser.parse_args()
    
    calculator = TradingLossCalculator()
    
    try:
        # 如果没有提供文件名参数，则自动查找最新文件
        if not args.file1 and not args.file2:
            calculator.logger.info("🔍 未提供文件名参数，自动查找最新的volume_stats文件...")
            file1, file2 = calculator.find_latest_volume_stats_files()
            
            if not file1 or not file2:
                calculator.logger.error("❌ 无法自动找到足够的文件，请手动指定文件名")
                return
        else:
            file1 = args.file1
            file2 = args.file2
        
        # 检查文件是否存在
        if not os.path.exists(file1):
            print(f"❌ 文件不存在: {file1}")
            return
        
        if not os.path.exists(file2):
            print(f"❌ 文件不存在: {file2}")
            return
        
        if calculator.load_csv_files(file1, file2):
            calculator.calculate_loss_analysis()
        else:
            print("❌ 无法加载CSV文件，请检查文件格式")
            
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")

if __name__ == "__main__":
    main()