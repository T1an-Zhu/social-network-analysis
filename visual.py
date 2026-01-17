import pandas as pd
import matplotlib.pyplot as plt

# 读取你跑出来的结果
df = pd.read_csv('kleinberg_star_beauties.csv')
top_1 = df.iloc[13] # 取第一名

# 解析引证历史
history = {int(item.split(':')[0]): int(item.split(':')[1]) 
           for item in top_1['citation_history'].split('; ')}

years = sorted(history.keys())
counts = [history[y] for y in years]

plt.figure(figsize=(12, 5))
plt.plot(years, counts, marker='o', color='#1f77b4', linewidth=2, label='Annual Citations')
plt.axvline(x=top_1['awakening_year'], color='red', linestyle='--', label=f"Awakening Point ({int(top_1['awakening_year'])})")
plt.fill_between(years, counts, color='skyblue', alpha=0.3)

plt.title(f"Patent {top_1['target_patent_id']} Citation Growth (Substantive Gap: {top_1['substantive_gap']} years)")
plt.xlabel('Year')
plt.ylabel('Citation Count')
plt.legend()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()