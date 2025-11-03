import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 正确的参数设置
T_AT = 768.92   # 市场总交易额预测(M)
T_NB = 383.42
T_HEMI = 258.99

R_AT = 770000
R_NB = 400000  
R_HEMI = 200000

cost_rate = 0.0004

# AT已刷交易额
AT_already_volume = 31.04  # M

# 正确的净收益函数
def calculate_net(V, T, R, account_limit_ratio=None, already_volume=0):
    # 总刷量 = 已刷 + 新刷
    total_volume = already_volume + V
    
    # 计算我们的份额
    our_share = total_volume / (T + total_volume)
    
    # 计算理论奖励
    theoretical_reward = our_share * R
    
    # 如果有账号上限，应用上限
    if account_limit_ratio is not None:
        actual_reward = min(theoretical_reward, account_limit_ratio * R)
    else:
        actual_reward = theoretical_reward
    
    # 成本只计算新刷的部分
    cost = V * 1e6 * cost_rate
    
    net_profit = actual_reward - cost
    return net_profit, actual_reward, our_share

# 生成更密集的数据点
V_values = np.arange(0, 301, 5)  # 0-300M，每5M一个点
data = []

for V in V_values:
    # AT: 9账号上限 27%
    at_net, at_reward, at_share = calculate_net(V, T_AT, R_AT, 0.27, AT_already_volume)
    
    # NB: 19账号上限 57% (19 * 3%)
    nb_net, nb_reward, nb_share = calculate_net(V, T_NB, R_NB, 0.57, 0)
    
    # HEMI: 无上限
    hemi_net, hemi_reward, hemi_share = calculate_net(V, T_HEMI, R_HEMI, None, 0)
    
    data.append({
        'V_M': V,
        'AT_Net': at_net,
        'AT_Reward': at_reward,
        'AT_Share': at_share,
        'NB_Net': nb_net,
        'NB_Reward': nb_reward, 
        'NB_Share': nb_share,
        'HEMI_Net': hemi_net,
        'HEMI_Reward': hemi_reward,
        'HEMI_Share': hemi_share
    })

df = pd.DataFrame(data)

# 计算斜率（导数）
df['AT_Slope'] = np.gradient(df['AT_Net'], df['V_M']) /100
df['NB_Slope'] = np.gradient(df['NB_Net'], df['V_M']) /100
df['HEMI_Slope'] = np.gradient(df['HEMI_Net'], df['V_M']) /100

df.to_csv('token_rewards_with_slope.csv', index=False)
print("带斜率数据的CSV文件已生成: token_rewards_with_slope.csv")

# 创建两个子图
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))

# 第一个子图：净收益曲线
ax1.plot(df['V_M'], df['AT_Net'], label='AT', linewidth=2.5, color='blue')
ax1.plot(df['V_M'], df['NB_Net'], label='NB', linewidth=2.5, color='red')
ax1.plot(df['V_M'], df['HEMI_Net'], label='HEMI (No Limit)', linewidth=2.5, color='green')

# 标记最大值点
at_max_idx = df['AT_Net'].idxmax()
nb_max_idx = df['NB_Net'].idxmax() 
hemi_max_idx = df['HEMI_Net'].idxmax()

# AT标注
ax1.annotate(f'AT Max: ${df.iloc[at_max_idx]["AT_Net"]:,.0f}\n(V={df.iloc[at_max_idx]["V_M"]}M)',
             xy=(df.iloc[at_max_idx]['V_M'], df.iloc[at_max_idx]['AT_Net']),
             xytext=(20, 20), textcoords='offset points',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='lightblue', alpha=0.7),
             arrowprops=dict(arrowstyle='->', color='blue'))

# NB标注
ax1.annotate(f'NB Max: ${df.iloc[nb_max_idx]["NB_Net"]:,.0f}\n(V={df.iloc[nb_max_idx]["V_M"]}M)',
             xy=(df.iloc[nb_max_idx]['V_M'], df.iloc[nb_max_idx]['NB_Net']),
             xytext=(-80, 20), textcoords='offset points',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='lightcoral', alpha=0.7),
             arrowprops=dict(arrowstyle='->', color='red'))

# HEMI标注
ax1.annotate(f'HEMI Max: ${df.iloc[hemi_max_idx]["HEMI_Net"]:,.0f}\n(V={df.iloc[hemi_max_idx]["V_M"]}M)',
             xy=(df.iloc[hemi_max_idx]['V_M'], df.iloc[hemi_max_idx]['HEMI_Net']),
             xytext=(20, -30), textcoords='offset points',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgreen', alpha=0.7),
             arrowprops=dict(arrowstyle='->', color='green'))

ax1.set_xlabel('Additional Trading Volume (Million USD)', fontsize=12)
ax1.set_ylabel('Net Profit (USD)', fontsize=12)
ax1.set_title('Net Profit vs Additional Trading Volume', fontsize=14, fontweight='bold')
ax1.grid(True, alpha=0.3)
ax1.legend(fontsize=11)
ax1.set_xlim(0, 300)
ax1.set_ylim(-50000, 150000)
ax1.axhline(y=0, color='black', linestyle='--', alpha=0.5, linewidth=1)
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000:,.0f}K'))

# 第二个子图：斜率曲线
ax2.plot(df['V_M'], df['AT_Slope'], label='AT Slope', linewidth=2, color='blue', linestyle='--')
ax2.plot(df['V_M'], df['NB_Slope'], label='NB Slope', linewidth=2, color='red', linestyle='--')
ax2.plot(df['V_M'], df['HEMI_Slope'], label='HEMI Slope', linewidth=2, color='green', linestyle='--')

# 标记斜率为0的点（净收益最大值点）
ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5, linewidth=1)
ax2.axvline(x=df.iloc[at_max_idx]['V_M'], color='blue', linestyle=':', alpha=0.7, label=f'AT Max at {df.iloc[at_max_idx]["V_M"]}M')
ax2.axvline(x=df.iloc[nb_max_idx]['V_M'], color='red', linestyle=':', alpha=0.7, label=f'NB Max at {df.iloc[nb_max_idx]["V_M"]}M')
ax2.axvline(x=df.iloc[hemi_max_idx]['V_M'], color='green', linestyle=':', alpha=0.7, label=f'HEMI Max at {df.iloc[hemi_max_idx]["V_M"]}M')

ax2.set_xlabel('Additional Trading Volume (Million USD)', fontsize=12)
ax2.set_ylabel('Slope (ΔNet Profit / ΔVolume)', fontsize=12)
ax2.set_title('Marginal Net Profit (Slope) vs Additional Trading Volume', fontsize=14, fontweight='bold')
ax2.grid(True, alpha=0.3)
ax2.legend(fontsize=11)
ax2.set_xlim(0, 300)
ax2.set_ylim(-5, 10)

plt.tight_layout()
plt.savefig('trading_volume_profit_with_slope.png', dpi=300, bbox_inches='tight')
plt.show()

# 打印斜率分析
print("=== Slope Analysis ===")
print("Slope represents the marginal net profit per additional $1M trading volume")
print("\nKey Slope Points:")

# 找到斜率接近0的点（最优刷额点）
at_zero_slope_idx = (df['AT_Slope'].abs()).idxmin()
nb_zero_slope_idx = (df['NB_Slope'].abs()).idxmin()
hemi_zero_slope_idx = (df['HEMI_Slope'].abs()).idxmin()

print(f"\nAT:")
print(f"  Optimal Volume: {df.iloc[at_max_idx]['V_M']}M")
print(f"  Slope at optimal: {df.iloc[at_max_idx]['AT_Slope']:.2f}")
print(f"  Initial slope: {df.iloc[0]['AT_Slope']:.2f}")
print(f"  Final slope: {df.iloc[-1]['AT_Slope']:.2f}")

print(f"\nNB:")
print(f"  Optimal Volume: {df.iloc[nb_max_idx]['V_M']}M")
print(f"  Slope at optimal: {df.iloc[nb_max_idx]['NB_Slope']:.2f}")
print(f"  Initial slope: {df.iloc[0]['NB_Slope']:.2f}")
print(f"  Final slope: {df.iloc[-1]['NB_Slope']:.2f}")

print(f"\nHEMI:")
print(f"  Optimal Volume: {df.iloc[hemi_max_idx]['V_M']}M")
print(f"  Slope at optimal: {df.iloc[hemi_max_idx]['HEMI_Slope']:.2f}")
print(f"  Initial slope: {df.iloc[0]['HEMI_Slope']:.2f}")
print(f"  Final slope: {df.iloc[-1]['HEMI_Slope']:.2f}")

# 打印决策建议
print("\n=== Investment Decision Insights ===")
print("When slope > 0: Increasing volume increases net profit")
print("When slope = 0: Optimal point (maximum net profit)")
print("When slope < 0: Increasing volume decreases net profit")

print(f"\nAT: Stop increasing when slope turns negative around {df.iloc[at_max_idx]['V_M']}M")
print(f"NB: Stop increasing when slope turns negative around {df.iloc[nb_max_idx]['V_M']}M")
print(f"HEMI: Stop increasing when slope turns negative around {df.iloc[hemi_max_idx]['V_M']}M")

# 显示前10行数据
print("\n=== Data Sample (First 10 rows) ===")
print(df[['V_M', 'AT_Net', 'AT_Slope', 'NB_Net', 'NB_Slope', 'HEMI_Net', 'HEMI_Slope']].head(10).round(2))