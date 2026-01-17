import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import numpy as np

# ================= 配置区 =================
NODE_FILE = 'expanded_diffusion_nodes.csv'
EDGE_FILE = 'expanded_diffusion_edges.csv'
OUTPUT_HTML = 'patent_network_interactive.html'
# ==========================================

def plot_stunning_network():
    # 1. 加载数据
    print("正在读取数据...")
    nodes_df = pd.read_csv(NODE_FILE)
    edges_df = pd.read_csv(EDGE_FILE)

    # 2. 创建 NetworkX 图对象
    G = nx.DiGraph()
    
    # 首先添加点表中的节点
    for _, row in nodes_df.iterrows():
        G.add_node(str(row['ID']), layer=str(row['Layer']), weight=row.get('Weight', 1))
    
    # 添加边，并处理那些在点表中不存在的节点
    for _, row in edges_df.iterrows():
        src, tgt = str(row['Source']), str(row['Target'])
        # 容错处理：如果节点没在点表中定义属性，赋予默认值
        if src not in G:
            G.add_node(src, layer='Diffusion_L3', weight=1)
        if tgt not in G:
            G.add_node(tgt, layer='Diffusion_L3', weight=1)
        G.add_edge(src, tgt)

    # 3. 计算布局
    # 使用 k 调大点之间的距离，让图散开
    print("正在计算美化布局（节点较多，请稍候）...")
    pos = nx.spring_layout(G, k=0.15, iterations=30, seed=42)

    # 4. 定义颜色映射
    color_map = {
        'Core': '#EF553B',       # 鲜红
        'Awakener': '#FECB52',   # 亮金
        'Citing_L2': '#636EFA',  # 宝蓝
        'Diffusion_L3': '#AB63FA' # 丁香紫
    }

    # 5. 准备边的绘图数据
    edge_x, edge_y = [], []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.4, color='#A1B5D8'), # 使用淡蓝色
        hoverinfo='none',
        mode='lines',
        opacity=0.5
    )

    # 6. 分层绘制节点
    node_traces = []
    # 获取图中所有存在的 layer 类别
    existing_layers = set(nx.get_node_attributes(G, 'layer').values())
    
    for layer in ['Core', 'Awakener', 'Citing_L2', 'Diffusion_L3']:
        if layer not in existing_layers: continue
        
        # 筛选属于该层的节点
        layer_nodes = [n for n, attr in G.nodes(data=True) if attr.get('layer') == layer]
        
        nx_list = [pos[n][0] for n in layer_nodes]
        ny_list = [pos[n][1] for n in layer_nodes]
        
        # 动态计算大小
        sizes = []
        for n in layer_nodes:
            if layer == 'Core': s = 45
            elif layer == 'Awakener': s = 25
            elif layer == 'Citing_L2': s = 10
            else: s = 4
            sizes.append(s)

        trace = go.Scatter(
            x=nx_list, y=ny_list,
            mode='markers',
            name=f"{layer} (n={len(layer_nodes)})",
            marker=dict(
                size=sizes, 
                color=color_map.get(layer, '#888'),
                line=dict(width=0.5, color='white')
            ),
            text=[f"专利号: {n}<br>层次: {layer}" for n in layer_nodes],
            hoverinfo='text'
        )
        node_traces.append(trace)

# 7. 构建画布
    fig = go.Figure(data=[edge_trace] + node_traces)

    fig.update_layout(
        title={
            'text': '<b>US4901362 技术扩散多级网络图 (交互式)</b>',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': dict(size=20) # 修正：titlefont_size 现在的正确写法
        },
        showlegend=True,
        hovermode='closest',
        margin=dict(b=0, l=0, r=0, t=60), # 留出顶部空间给标题
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='white'
    )

    # 8. 保存并自动打开
    fig.write_html(OUTPUT_HTML)
    print(f"\n>>> 可视化成功！")
    print(f">>> 文件已保存至: {OUTPUT_HTML}")
    print(f">>> 建议使用 Chrome 浏览器打开，效果最佳。")
    fig.show()

if __name__ == "__main__":
    plot_stunning_network()