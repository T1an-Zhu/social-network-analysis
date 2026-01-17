import pandas as pd
import zipfile

# ================= 配置区 =================
ai_id_file = 'comprehensive_ai_patent_ids.csv'
citation_zip_path = 'g_us_patent_citation.tsv.zip'
output_file = 'ai_patent_citation_links.csv'
target_cols = ['patent_id', 'citation_patent_id', 'citation_date']
# ==========================================

def extract_ai_citations():
    print(f"正在读取 AI 专利清单...")
    ai_ids_df = pd.read_csv(ai_id_file, dtype={'patent_id': str})
    ai_ids_set = set(ai_ids_df['patent_id'].unique())
    print(f"清单加载完成，共有 {len(ai_ids_set)} 个核心 AI 专利。")

    print(f"开始处理压缩包...")
    chunk_count = 0
    total_matches = 0

    try:
        with zipfile.ZipFile(citation_zip_path, 'r') as z:
            # 自动寻找压缩包内真正的 TSV 文件
            all_files = z.namelist()
            # 过滤掉文件夹和非 tsv 文件，按文件名长度或包含关系寻找
            tsv_files = [f for f in all_files if f.endswith('.tsv') and not f.startswith('__MACOSX')]
            
            if not tsv_files:
                print("错误：压缩包内未找到 .tsv 文件！")
                print("压缩包内的文件列表为:", all_files)
                return
            
            # 选择第一个匹配的 tsv 文件
            target_tsv = tsv_files[0]
            print(f"锁定目标文件: {target_tsv}")
            
            with z.open(target_tsv) as f:
                reader = pd.read_csv(
                    f, 
                    sep='\t', 
                    chunksize=500000, # 增加到50万行一块，提高效率
                    low_memory=False,
                    usecols=target_cols,
                    dtype={'patent_id': str, 'citation_patent_id': str}
                )

                first_chunk = True
                for chunk in reader:
                    chunk_count += 1
                    
                    # 匹配被引 ID
                    is_match = chunk['citation_patent_id'].isin(ai_ids_set)
                    match_data = chunk[is_match]
                    
                    if not match_data.empty:
                        total_matches += len(match_data)
                        mode = 'w' if first_chunk else 'a'
                        header = True if first_chunk else False
                        match_data.to_csv(output_file, mode=mode, index=False, header=header)
                        first_chunk = False
                    
                    if chunk_count % 10 == 0:
                        print(f"已扫描 {chunk_count * 500000 / 1000000:.1f} 百万行... 已捕获 {total_matches} 条记录")

        print("-" * 30)
        print(f"筛选完成！最终提取到 {total_matches} 条记录。")
        print(f"结果已存入: {output_file}")

    except Exception as e:
        print(f"运行出错: {e}")

if __name__ == "__main__":
    extract_ai_citations()