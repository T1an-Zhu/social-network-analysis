import pandas as pd
import plotly.graph_objects as go
from collections import Counter
import zipfile
import io
import os

# ================= 配置区 =================
EDGE_FILE = 'expanded_diffusion_edges.csv'
FILE_CPC = 'g_cpc_current.tsv.zip'
OUTPUT_HTML = 'cpc_real_transition.html'
OUTPUT_TXT = 'cpc_analysis_report.txt'

# 定义你的核心 Awaker 专利号（请确保这些 ID 在边文件中存在）
# 如果你不确定，程序会自动选取边文件中出度最高的前5个作为核心
AWAKER_IDS = ['4901362'] # 你可以根据之前的分析在这里添加更多核心 ID
# ==========================================

def plot_focused_cpc_pathway():
    if not os.path.exists(EDGE_FILE):
        print(f"错误：找不到文件 {EDGE_FILE}")
        return
        
    print(">>> 正在提取核心 Awaker 的扩散路径...")
    edges_df = pd.read_csv(EDGE_FILE)
    edges_df['Source'] = edges_df['Source'].astype(str).str.strip('"')
    edges_df['Target'] = edges_df['Target'].astype(str).str.strip('"')
    
    # 获取网络中所有的专利 ID
    relevant_patents = set(edges_df['Source']) | set(edges_df['Target'])
    
    # 1. 匹配 CPC 数据（复用你成功的流式读取逻辑）
    cpc_map = {}
    try:
        with zipfile.ZipFile(FILE_CPC) as z:
            with z.open(z.namelist()[0]) as f:
                raw_line = f.readline().decode('utf-8').strip()
                header = [h.strip().strip('"').replace('\ufeff', '') for h in raw_line.split('\t')]
                pid_col = next((h for h in header if h in ['patent_id', 'patent_number', 'id']), None)
                sub_col = next((h for h in header if h in ['cpc_subclass', 'cpc_group', 'subsection_id']), None)

                f.seek(0)
                reader = pd.read_csv(f, sep='\t', chunksize=500000, low_memory=False, dtype=str, quotechar='"')
                for chunk in reader:
                    chunk.columns = [c.strip().strip('"') for c in chunk.columns]
                    subset = chunk[[pid_col, sub_col]]
                    mask = subset[pid_col].isin(relevant_patents)
                    matches = subset[mask]
                    for _, row in matches.iterrows():
                        pid, val = row[pid_col], row[sub_col]
                        if pd.notna(val) and len(val) >= 4:
                            val = val.split(' ')[0][:4]
                        if pid not in cpc_map:
                            cpc_map[pid] = val
                    if len(cpc_map) >= len(relevant_patents): break
    except Exception as e:
        print(f"数据读取出错: {e}")
        return

    # 2. 筛选 Awaker 相关的迁移路径
    # 如果没指定 AWAKER_IDS，则自动识别被引最多的前几个
    target_ids = AWAKER_IDS if AWAKER_IDS else edges_df['Target'].value_counts().head(5).index.tolist()
    
    transitions = []
    report_lines = ["专利技术扩散路径分析报告", ""]

    for awaker in target_ids:
        # 找到所有引用了该 Awaker 的施引专利
        citers = edges_df[edges_df['Target'] == awaker]['Source'].tolist()
        awaker_cpc = cpc_map.get(awaker, "Unknown")
        
        # 统计这些施引专利分别属于哪些领域
        citer_cpcs = [cpc_map.get(c) for c in citers if cpc_map.get(c)]
        cpc_counts = Counter(citer_cpcs)
        
        for target_cpc, count in cpc_counts.items():
            if count >= 1: # 只要有引用就记录
                transitions.append({
                    'awaker': awaker,
                    'source_field': awaker_cpc,
                    'target_field': target_cpc,
                    'weight': count
                })
                report_lines.append(f"核心专利 {awaker} ({awaker_cpc}) 扩散至 {target_cpc} 领域，路径权重为 {count}")

    # 3. 可视化：使用更清晰的散点连接图
    fig = go.Figure()

    # 提取唯一的领域节点
    all_fields = list(set([t['source_field'] for t in transitions] + [t['target_field'] for t in transitions]))
    # 简单的左右布局：左侧是起始领域，右侧是扩散领域
    field_coords = {f: i for i, f in enumerate(all_fields)}
    
    for t in transitions:
        fig.add_trace(go.Scatter(
            x=[0, 1], 
            y=[field_coords[t['source_field']], field_coords[t['target_field']]],
            mode='lines+markers+text',
            line=dict(width=t['weight']*0.8, color='rgba(31, 119, 180, 0.5)'),
            text=[f"Awaker: {t['awaker']}", f"{t['target_field']} (权重:{t['weight']})"],
            textposition="top center",
            hoverinfo='text'
        ))

    fig.update_layout(
        title="核心 Awaker 技术迁移路径图 (标注权重)",
        xaxis=dict(title="扩散阶段 (起始 -> 扩散)", showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        showlegend=False,
        plot_bgcolor='white'
    )
    
    # 4. 输出
    fig.write_html(OUTPUT_HTML)
    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines))

    print(f"\n>>> 任务完成！")
    print(f"可视化文件已保存为：{OUTPUT_HTML}（打开后每条线代表一个 Awaker 的扩散路径，线越粗权重越大）")
    print(f"分析报告已保存为：{OUTPUT_TXT}")
    fig.show()

if __name__ == "__main__":
    plot_focused_cpc_pathway()