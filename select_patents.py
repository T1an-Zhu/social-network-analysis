import pandas as pd
import zipfile
import io

# ================= 配置区 =================
# 1. 你的压缩包完整文件名
zip_file_path = 'g_ipc_at_issue.tsv.zip' 

# 2. 导出结果的文件名
output_file = 'comprehensive_ai_patent_ids.csv'

# 3. 定义全年代 AI IPC 核心索引 (拼接后无空格模式)
ai_prefixes = (
    'G06N',        # 核心 AI：机器学习、神经网络、量子计算、专家系统
    'G06F15/18',   # 80-90年代：旧版机器学习类目
    'G06K9',       # 传统模式识别：图像识别、指纹等（2022年后转移到G06V）
    'G06V',        # 现代计算机视觉：2022年后的新类目
    'G06T7',       # 图像分析：特征提取、运动检测
    'G06F17/30',   # 信息检索：虽然广泛，但涉及早期的搜索算法
    'G06F40',      # 自然语言处理 (NLP)：处理文本、语音识别的基础
    'B25J9/16',    # 智能控制：工业机器人的自适应控制
    'G05B13',      # 智能控制：自适应/启发式控制系统
    'G01S',        # 传感器融合：自动驾驶感知的核心（如雷达、激光雷达）
    'G10L',        # 语音识别与合成
    'B60W30',      # 自动驾驶：辅助驾驶系统的决策与控制
)
# ==========================================

def process_tsv_from_zip():
    print(f"开始打开压缩包: {zip_file_path}")
    ai_id_set = set()
    chunk_count = 0

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as z:
            # 自动获取压缩包内 tsv 的文件名
            tsv_names = [f for f in z.namelist() if f.endswith('.tsv')]
            if not tsv_names:
                print("错误：压缩包内未找到 .tsv 文件")
                return
            
            target_tsv = tsv_names[0]
            print(f"检测到内部文件: {target_tsv}，正在流式读取...")

            # 使用 z.open 直接读取字节流，不占用额外硬盘空间
            with z.open(target_tsv) as f:
                # pandas 通过 chunksize 分块读取
                reader = pd.read_csv(
                    f, 
                    sep='\t', 
                    chunksize=100000, 
                    low_memory=False, 
                    # 只取关键列，大幅降低内存压力
                    usecols=['patent_id', 'section', 'ipc_class', 'subclass', 'main_group', 'subgroup']
                )

                for chunk in reader:
                    chunk_count += 1
                    
                    # 1. 字段清洗：去除空格并转为字符串
                    for col in ['section', 'ipc_class', 'subclass', 'main_group', 'subgroup']:
                        chunk[col] = chunk[col].astype(str).str.strip().str.replace('nan', '', case=False)

                    # 2. 拼接完整的 IPC 编码 (例如 G06N3/04)
                    chunk['full_ipc'] = chunk['section'] + chunk['ipc_class'] + chunk['subclass'] + \
                                        chunk['main_group'] + chunk['subgroup']

                    # 3. 匹配 AI 前缀
                    is_ai = chunk['full_ipc'].str.startswith(ai_prefixes)
                    
                    # 4. 收集匹配成功的 patent_id
                    match_ids = chunk.loc[is_ai, 'patent_id'].unique()
                    ai_id_set.update(match_ids)
                    
                    if chunk_count % 10 == 0:
                        print(f"已扫描 {chunk_count * 100000} 行数据... 已找到 {len(ai_id_set)} 个候选专利")

        # 5. 保存结果
        result_df = pd.DataFrame({'patent_id': list(ai_id_set)})
        result_df.to_csv(output_file, index=False)
        
        print("-" * 30)
        print(f"筛选完成！共处理 {chunk_count * 100000} 行数据。")
        print(f"最终提取出 AI 相关专利: {len(ai_id_set)} 条。")
        print(f"结果已保存至: {output_file}")

    except FileNotFoundError:
        print(f"错误：找不到文件 {zip_file_path}，请确保它在脚本所在目录下。")
    except Exception as e:
        print(f"发生未知错误: {e}")

if __name__ == "__main__":
    process_tsv_from_zip()