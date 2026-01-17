import pandas as pd
import zipfile

# ================= 配置区 =================
TARGET_PATENT = '4901362'
FILE_CITATION = 'g_us_patent_citation.tsv.zip'  
FILE_PATENT = 'g_patent.tsv.zip'                
FILE_ASSIGNEE = 'g_assignee_disambiguated.tsv.zip' 
FILE_CPC = 'g_cpc_current.tsv.zip'              
OUTPUT_FILE = 'citation_analysis_4901362_final.csv'
# ==========================================

def get_depth_data():
    # 1. 提取施引专利号
    print(f"步骤 1: 正在从引证库搜索引用了 {TARGET_PATENT} 的专利...")
    citing_ids = set()
    with zipfile.ZipFile(FILE_CITATION) as z:
        with z.open(z.namelist()[0]) as f:
            # 根据你提供的错误信息，准确使用以下列名：
            # patent_id 是施引者，citation_patent_id 是被引者
            col_citing = 'patent_id'
            col_cited = 'citation_patent_id'
            
            reader = pd.read_csv(f, sep='\t', chunksize=1000000, low_memory=False, 
                                 usecols=[col_citing, col_cited])
            for chunk in reader:
                # 确保匹配时类型一致
                chunk[col_cited] = chunk[col_cited].astype(str)
                matches = chunk[chunk[col_cited] == TARGET_PATENT]
                if not matches.empty:
                    citing_ids.update(matches[col_citing].astype(str).tolist())
    
    print(f">>> 发现 {len(citing_ids)} 条施引记录。")
    if not citing_ids:
        print("未发现施引记录，请检查目标专利号是否正确。")
        return

    results = {pid: {'year': 'N/A', 'assignee': 'Individual/Unknown', 'cpc': []} for pid in citing_ids}

    # 2. 关联授权年份
    print("步骤 2: 正在关联施引专利的授权年份...")
    with zipfile.ZipFile(FILE_PATENT) as z:
        with z.open(z.namelist()[0]) as f:
            # 通常基础表是 patent_id 或 id
            header = pd.read_csv(f, sep='\t', nrows=0).columns.tolist()
            pid_col = 'patent_id' if 'patent_id' in header else 'id'
            date_col = 'patent_date' if 'patent_date' in header else 'date'
            
            f.seek(0)
            reader = pd.read_csv(f, sep='\t', chunksize=1000000, low_memory=False, usecols=[pid_col, date_col])
            for chunk in reader:
                chunk[pid_col] = chunk[pid_col].astype(str)
                mask = chunk[pid_col].isin(citing_ids)
                for _, row in chunk[mask].iterrows():
                    results[row[pid_col]]['year'] = str(row[date_col])[:4]

    # 3. 关联申请人
    print("步骤 3: 正在关联消歧后的申请人名称...")
    with zipfile.ZipFile(FILE_ASSIGNEE) as z:
        with z.open(z.namelist()[0]) as f:
            header = pd.read_csv(f, sep='\t', nrows=0).columns.tolist()
            pid_col = 'patent_id' if 'patent_id' in header else 'id'
            # 消歧后的组织名称列
            org_col = 'organization' if 'organization' in header else 'disambig_assignee_organization'
            
            f.seek(0)
            reader = pd.read_csv(f, sep='\t', chunksize=1000000, low_memory=False, usecols=[pid_col, org_col])
            for chunk in reader:
                chunk[pid_col] = chunk[pid_col].astype(str)
                mask = chunk[pid_col].isin(citing_ids)
                for _, row in chunk[mask].iterrows():
                    if pd.notna(row[org_col]):
                        results[row[pid_col]]['assignee'] = row[org_col]

    # 4. 关联 CPC
    print("步骤 4: 正在关联 CPC 技术领域...")
    with zipfile.ZipFile(FILE_CPC) as z:
        with z.open(z.namelist()[0]) as f:
            header = pd.read_csv(f, sep='\t', nrows=0).columns.tolist()
            pid_col = 'patent_id' if 'patent_id' in header else 'id'
            cpc_col = 'cpc_group' if 'cpc_group' in header else 'group_id'
            
            f.seek(0)
            reader = pd.read_csv(f, sep='\t', chunksize=1000000, low_memory=False, usecols=[pid_col, cpc_col])
            for chunk in reader:
                chunk[pid_col] = chunk[pid_col].astype(str)
                mask = chunk[pid_col].isin(citing_ids)
                for _, row in chunk[mask].iterrows():
                    results[row[pid_col]]['cpc'].append(str(row[cpc_col]))

    # 5. 输出
    print("步骤 5: 正在生成最终文件...")
    output_list = []
    for pid, info in results.items():
        output_list.append({
            'Citing_Patent': pid,
            'Year': info['year'],
            'Assignee': info['assignee'],
            'CPC_Groups': "; ".join(sorted(list(set(info['cpc']))))
        })

    pd.DataFrame(output_list).sort_values('Year').to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"完成！请查看: {OUTPUT_FILE}")

if __name__ == "__main__":
    get_depth_data()