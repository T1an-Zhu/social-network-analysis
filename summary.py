import pandas as pd
import zipfile
import os

# ================= 配置区 =================
links_file = 'ai_patent_citation_links.csv'     
patent_info_zip = 'g_patent.tsv.zip'           
output_file = 'ai_patent_summary.csv'
# ==========================================

def analyze_sleeping_beauty_robust():
    # ---- 第一步：分块读取 g_patent 并只保留 ID 和 年份 ----
    print("正在从 g_patent 提取年份信息（分块读取模式）...")
    temp_year_list = []
    
    with zipfile.ZipFile(patent_info_zip, 'r') as z:
        target_tsv = [f for f in z.namelist() if f.endswith('.tsv')][0]
        with z.open(target_tsv) as f:
            # 增加 chunksize 以减少循环次数，增加稳定性
            reader = pd.read_csv(f, sep='\t', chunksize=1000000, 
                                 usecols=['patent_id', 'patent_date'], 
                                 dtype={'patent_id': str})
            for chunk in reader:
                # 转换日期并只保留年份
                chunk['year'] = pd.to_datetime(chunk['patent_date'], errors='coerce').dt.year
                # 只要这两列，减少内存占用
                temp_year_list.append(chunk[['patent_id', 'year']].dropna())

    # 合并成一个轻量级的年份对照表
    year_df = pd.concat(temp_year_list, ignore_index=True)
    del temp_year_list # 释放内存
    print(f"年份索引构建完成，共记录 {len(year_df)} 条专利。")

    # ---- 第二步：加载引证关系 ----
    print("加载引证关系文件...")
    links_df = pd.read_csv(links_file, dtype={'patent_id': str, 'citation_patent_id': str})

    # ---- 第三步：通过 Merge 匹配施引年份 (citing_year) ----
    print("匹配施引年份（Merge 模式）...")
    # 这里通过 patent_id 关联，获取引用的年份
    links_df = pd.merge(links_df, year_df, left_on='patent_id', right_on='patent_id', how='left')
    links_df.rename(columns={'year': 'citing_year'}, inplace=True)
    
    # 剔除无法匹配日期的数据
    links_df = links_df.dropna(subset=['citing_year'])
    links_df['citing_year'] = links_df['citing_year'].astype(int)

    # ---- 第四步：计算每年的被引频次 ----
    print("统计年度引证分布...")
    yearly_counts = links_df.groupby(['citation_patent_id', 'citing_year']).size().reset_index(name='count')
    yearly_counts['history_item'] = yearly_counts['citing_year'].astype(str) + ":" + yearly_counts['count'].astype(str)

    # ---- 第五步：汇总最终结构 ----
    print("归一化汇总...")
    final_summary = yearly_counts.groupby('citation_patent_id').agg(
        total_citations=('count', 'sum'),
        citation_history=('history_item', lambda x: "; ".join(x))
    ).reset_index()
    final_summary.rename(columns={'citation_patent_id': 'target_patent_id'}, inplace=True)

    # ---- 第六步：再次通过 Merge 匹配目标专利的出生年份 (birth_year) ----
    print("匹配目标专利出生年份...")
    final_summary = pd.merge(final_summary, year_df, left_on='target_patent_id', right_on='patent_id', how='left')
    final_summary.rename(columns={'year': 'birth_year'}, inplace=True)
    
    # 清洗出生年份
    final_summary['birth_year'] = final_summary['birth_year'].fillna(0).astype(int)

    # 整理列顺序并保存
    final_summary = final_summary[['target_patent_id', 'birth_year', 'total_citations', 'citation_history']]
    final_summary.to_csv(output_file, index=False)
    
    print("-" * 30)
    print(f"处理成功！结果已保存至: {output_file}")

if __name__ == "__main__":
    analyze_sleeping_beauty_robust()