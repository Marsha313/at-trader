import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 参数设置
T_AT = 768.92   # 其他交易额(M)
T_NB = 383.42
T_HEMI = 258.99

R_AT = 770000
R_NB = 400000  
R_HEMI = 200000

cost_rate = 0.0004

# 净收益函数 - HEMI无奖励上限
def calculate_net(V, T, R, account_limit_ratio=None):
    if account_limit_ratio is not None:
        # 有账号上限的情况
        reward = min(account_limit_ratio * R, (V / (T + V)) * R)
    else:
        # 无账号上限的情况 - 直接按份额计算奖励
        reward = (V / (T + V)) * R
    cost = V * 1e6 * cost_rate  # V单位是M，转换成美元计算成本
    return reward - cost

# 生成数据
V_values = np.arange(0, 501, 20)
data = []

for V in V_values:
    at_net = calculate_net(V, T_AT, R_AT, 0.27)    # 9账号 * 3% = 27%
    nb_net = calculate_net(V, T_NB, R_NB, 1.0)     # 总奖池400K < 账号上限480K
    hemi_net = calculate_net(V, T_HEMI, R_HEMI, None)  # None表示无奖励上限
    
    data.append({
        'V_M': V,
        'AT_Net': at_net,
        'NB_Net': nb_net, 
        'HEMI_Net': hemi_net
    })

df = pd.DataFrame(data)
df.to_csv('token_rewards_no_limit.csv', index=False)
print("CSV文件已生成: token_rewards_no_limit.csv")

# 读取数据并绘图
df = pd.read_csv('token_rewards_no_limit.csv')

# 创建图表
plt.figure(figsize=(12, 8))

# 绘制三条曲线
plt.plot(df['V_M'], df['AT_Net'], label='AT', linewidth=2.5, color='blue', marker='o', markersize=4)
plt.plot(df['V_M'], df['NB_Net'], label='NB', linewidth=2.5, color='red', marker='s', markersize=4)
plt.plot(df['V_M'], df['HEMI_Net'], label='HEMI (No Limit)', linewidth=2.5, color='green', marker='^', markersize=4)

# 标记最大值点
at_max_idx = df['AT_Net'].idxmax()
nb_max_idx = df['NB_Net'].idxmax() 
hemi_max_idx = df['HEMI_Net'].idxmax()

plt.annotate(f'AT Max: ${df.iloc[at_max_idx]["AT_Net"]:,.0f}\n(V={df.iloc[at_max_idx]["V_M"]}M)',
             xy=(df.iloc[at_max_idx]['V_M'], df.iloc[at_max_idx]['AT_Net']),
             xytext=(20, -20), textcoords='offset points',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='lightblue', alpha=0.7),
             arrowprops=dict(arrowstyle='->', color='blue'))

plt.annotate(f'NB Max: ${df.iloc[nb_max_idx]["NB_Net"]:,.0f}\n(V={df.iloc[nb_max_idx]["V_M"]}M)',
             xy=(df.iloc[nb_max_idx]['V_M'], df.iloc[nb_max_idx]['NB_Net']),
             xytext=(20, 20), textcoords='offset points',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='lightcoral', alpha=0.7),
             arrowprops=dict(arrowstyle='->', color='red'))

plt.annotate(f'HEMI Max: ${df.iloc[hemi_max_idx]["HEMI_Net"]:,.0f}\n(V={df.iloc[hemi_max_idx]["V_M"]}M)',
             xy=(df.iloc[hemi_max_idx]['V_M'], df.iloc[hemi_max_idx]['HEMI_Net']),
             xytext=(-80, 20), textcoords='offset points',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgreen', alpha=0.7),
             arrowprops=dict(arrowstyle='->', color='green'))

# 设置图表属性（英文）
plt.xlabel('Trading Volume (Million USD)', fontsize=12)
plt.ylabel('Net Profit (USD)', fontsize=12)
plt.title('Trading Volume vs Net Profit (HEMI No Reward Limit)', fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.legend(fontsize=11)

# 设置坐标轴范围
plt.xlim(0, 500)
plt.ylim(-150000, 200000)

# 添加零线
plt.axhline(y=0, color='black', linestyle='--', alpha=0.5, linewidth=1)

# 格式化y轴标签
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000:,.0f}K'))

plt.tight_layout()
plt.savefig('trading_volume_profit.png', dpi=300, bbox_inches='tight')
plt.show()

# 打印最优刷额建议
print("=== Optimal Trading Volume Recommendations (HEMI No Limit) ===")
print(f"AT:  Volume {df.iloc[at_max_idx]['V_M']}M, Net Profit ${df.iloc[at_max_idx]['AT_Net']:,.0f}")
print(f"NB:  Volume {df.iloc[nb_max_idx]['V_M']}M, Net Profit ${df.iloc[nb_max_idx]['NB_Net']:,.0f}")
print(f"HEMI: Volume {df.iloc[hemi_max_idx]['V_M']}M, Net Profit ${df.iloc[hemi_max_idx]['HEMI_Net']:,.0f}")

# 显示HEMI的详细数据
print("\n=== HEMI No Limit Details ===")
print(f"HEMI Total Reward: ${R_HEMI:,}")
print(f"HEMI Market Volume: {T_HEMI}M")
print(f"HEMI Share at Optimal: {df.iloc[hemi_max_idx]['V_M']/(T_HEMI + df.iloc[hemi_max_idx]['V_M']):.1%}")
print(f"HEMI Reward at Optimal: ${(df.iloc[hemi_max_idx]['V_M']/(T_HEMI + df.iloc[hemi_max_idx]['V_M'])) * R_HEMI:,.0f}")

# 显示数据表格前10行
print("\n=== Data Sample (First 10 rows) ===")
print(df.head(10).round(0))