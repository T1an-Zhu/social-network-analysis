import pandas as pd
import zipfile
from collections import Counter

# ================= 配置区 =================
TARGET_ID = '4901362'
FILE_CITATION = 'g_us_patent_citation.tsv.zip'
INPUT_CSV = 'citation_analysis_4901362_final.csv'
TOP_N_GIANTS = 20  # 选取前20个最强的施引专利进行“发散扩散”分析
OUTPUT_EDGES = 'expanded_diffusion_edges.csv'
OUTPUT_NODES = 'expanded_diffusion_nodes.csv'
# ==========================================

def build_advanced_diffusion_network():
    # 1. 加载一阶施引者
    df_step1 = pd.read_csv(INPUT_CSV)
    df_step1['Citing_Patent'] = df_step1['Citing_Patent'].astype(str)
    citing_ids = set(df_step1['Citing_Patent'].tolist())
    
    # 2. 确定“发散源”：从371个专利中选出全局被引最高的前N个
    # 注意：我们需要先跑一遍权重，或者根据你手头的 Global_Citations 排序
    # 这里假设我们先选出前 N 个作为发散中心
    print(f">>> 正在识别前 {TOP_N_GIANTS} 个‘巨人施引者’作为发散源...")
    
    # 3. 第一次扫描：获取权重并确定发散源
    global_counts = Counter()
    with zipfile.ZipFile(FILE_CITATION) as z:
        with z.open(z.namelist()[0]) as f:
            reader = pd.read_csv(f, sep='\t', chunksize=2000000, low_memory=False, usecols=['citation_patent_id'])
            for chunk in reader:
                chunk['citation_patent_id'] = chunk['citation_patent_id'].astype(str)
                mask = chunk['citation_patent_id'].isin(citing_ids)
                global_counts.update(chunk.loc[mask, 'citation_patent_id'].tolist())
    
    # 选出发散源 ID
    giants = [p for p, c in global_counts.most_common(TOP_N_GIANTS)]
    all_monitored_ids = citing_ids | {TARGET_ID}
    
    print(f">>> 确定的发散源包括: {giants[:5]}等")

    # 4. 第二次扫描：提取三层关系
    # 层1-2: 4901362 <-> 371人 以及 371人内部
    # 层3: 巨人 -> 全网追随者
    edges = []
    layer3_nodes = set()

    print(">>> 正在提取跨层连边（这可能需要较长时间）...")
    with zipfile.ZipFile(FILE_CITATION) as z:
        with z.open(z.namelist()[0]) as f:
            reader = pd.read_csv(f, sep='\t', chunksize=2000000, low_memory=False, 
                                 usecols=['patent_id', 'citation_patent_id'])
            for chunk in reader:
                chunk['patent_id'] = chunk['patent_id'].astype(str)
                chunk['citation_patent_id'] = chunk['citation_patent_id'].astype(str)
                
                # 情况 A: 内部连边 (1-2层内部)
                mask_internal = chunk['patent_id'].isin(all_monitored_ids) & chunk['citation_patent_id'].isin(all_monitored_ids)
                if mask_internal.any():
                    for _, row in chunk[mask_internal].iterrows():
                        edges.append({'Source': row['patent_id'], 'Target': row['citation_patent_id'], 'Type': 'Internal'})
                
                # 情况 B: 发散连边 (3层：谁引用了巨人)
                mask_diffusion = chunk['citation_patent_id'].isin(giants)
                if mask_diffusion.any():
                    # 采样：为了防止边太多，我们只取部分连边或全取
                    diff_matches = chunk[mask_diffusion]
                    for _, row in diff_matches.iterrows():
                        edges.append({'Source': row['patent_id'], 'Target': row['citation_patent_id'], 'Type': 'Diffusion'})
                        layer3_nodes.add(row['patent_id'])

    # 5. 构建最终节点表
    print(">>> 正在整合节点层次属性...")
    final_nodes = []
    # 核心专利
    final_nodes.append({'ID': TARGET_ID, 'Layer': 'Core', 'Weight': global_counts.get(TARGET_ID, 0)})
    # 施引者
    for p in citing_ids:
        layer = 'Awakener' if p in giants else 'Citing_L2'
        final_nodes.append({'ID': p, 'Layer': layer, 'Weight': global_counts.get(p, 0)})
    # 三阶发散节点
    for p in layer3_nodes:
        if p not in all_monitored_ids:
            final_nodes.append({'ID': p, 'Layer': 'Diffusion_L3', 'Weight': 1}) # L3通常只算局部展示

    # 6. 保存结果
    pd.DataFrame(edges).to_csv(OUTPUT_EDGES, index=False)
    pd.DataFrame(final_nodes).to_csv(OUTPUT_NODES, index=False)
    
    print("-" * 30)
    print(f"挑战成功！")
    print(f"边表: {OUTPUT_EDGES} (含内部重组及外部扩散)")
    print(f"点表: {OUTPUT_NODES} (已标记 Core/Awakener/Diffusion 层次)")

if __name__ == "__main__":
    build_advanced_diffusion_network()