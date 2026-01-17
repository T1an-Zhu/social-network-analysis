import pandas as pd
import numpy as np

def detect_burst_year(history_str, birth_year):
    """
    基于 Kleinberg 突变思想的觉醒点检测
    逻辑：检测引证频率的显著上升拐点
    """
    if pd.isna(history_str): return None
    
    # 1. 解析引证历史
    history = {}
    for item in str(history_str).split('; '):
        if ':' in item:
            y, c = item.split(':')
            history[int(y)] = int(c)
    if not history: return None
    
    years = sorted(history.keys())
    if not years: return None
    
    # 补齐年份序列（从出生到最后一次引用），缺失年记为 0
    full_range = range(int(birth_year), max(years) + 1)
    counts = [history.get(y, 0) for y in full_range]
    
    # 2. 寻找突发点 (Burst Point)
    # 计算移动平均作为背景基准
    # 突发点定义：当年引证量显著高于之前的累计平均水平，且后续保持增长
    cumulative_avg = 0
    for i, year in enumerate(full_range):
        if year <= birth_year: continue
        
        current_val = counts[i]
        # 背景水平：前序年份的平均引证
        background = np.mean(counts[:i]) if i > 0 else 0
        
        # 判定条件：
        # 1. 当前值大于背景水平的 2 倍（频率跃迁）
        # 2. 当前值必须达到一定的绝对量（防止低频波动的干扰）
        # 3. 必须在出生 3 年后（过滤初始噪音）
        if current_val > (background * 2 + 1) and current_val >= 3 and (year - birth_year) >= 3:
            # 验证后续 3 年的平均水平，确保不是单年随机波动（Kleinberg 状态维持思想）
            post_avg = np.mean(counts[i:i+3]) if i+3 <= len(counts) else current_val
            if post_avg >= current_val * 0.7: 
                return year
    return None

def calculate_b_coefficient(row):
    """计算 B 系数（偏移面积法）"""
    history = {}
    for item in str(row['citation_history']).split('; '):
        if ':' in item:
            y, c = item.split(':')
            history[int(y)] = int(c)
    if not history: return 0
    
    birth_year = row['birth_year']
    if pd.isna(birth_year) or birth_year == 0: return 0
    
    max_c = max(history.values())
    peak_year = [y for y, c in history.items() if c == max_c][0]
    
    if peak_year <= birth_year: return 0
    
    c_birth = history.get(birth_year, 0)
    b = 0
    for t in range(int(birth_year), int(peak_year) + 1):
        c_t = history.get(t, 0)
        l_t = ((max_c - c_birth) / (peak_year - birth_year)) * (t - birth_year) + c_birth
        b += (l_t - c_t)
    return b

def main():
    input_file = 'ai_patent_summary.csv' 
    output_file = 'kleinberg_star_beauties.csv'

    print("正在加载数据并执行突发检测与 B 指数计算...")
    df = pd.read_csv(input_file)

    # 1. 计算 B 系数
    df['B_index'] = df.apply(calculate_b_coefficient, axis=1)

    # 2. 执行突发检测寻找觉醒年份 (Burst Year)
    df['awakening_year'] = df.apply(lambda row: detect_burst_year(row['citation_history'], row['birth_year']), axis=1)

    # 3. 计算实质沉睡期 (Substantive Sleep Gap)
    df['substantive_gap'] = df['awakening_year'] - df['birth_year']

    # 4. 筛选明星案例
    # 过滤掉没有检测到觉醒点或沉睡期太短的
    mask = (df['birth_year'] <= 2005) & \
           (df['total_citations'] >= 30) & \
           (df['substantive_gap'] >= 10)
    
    stars = df[mask].copy()

    # 5. 分类标签
    def label_pattern(row):
        if row['total_citations'] > 150: return "泰坦型 (高影响力/突发性强)"
        if row['substantive_gap'] > 20: return "深海遗珠型 (极长沉睡/跨代突变)"
        return "典型睡美人"

    stars['research_label'] = stars.apply(label_pattern, axis=1)

    # 按 B 指数排序，取最有特点的 50 个
    stars = stars.sort_values('B_index', ascending=False).head(50)
    
    # 整理输出列
    output_cols = ['target_patent_id', 'birth_year', 'awakening_year', 'substantive_gap', 
                   'total_citations', 'B_index', 'research_label', 'citation_history']
    stars[output_cols].to_csv(output_file, index=False)
    
    print("-" * 30)
    print(f"处理完成！识别出具有显著突发特征的睡美人 {len(stars)} 个。")
    print(f"结果已存至: {output_file}")

if __name__ == "__main__":
    main()