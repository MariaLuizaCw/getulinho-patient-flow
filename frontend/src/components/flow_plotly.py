import networkx as nx
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import textwrap
from utils.theme import get_streamlit_theme



def combined_mean(means: pd.Series, counts: pd.Series) -> float:
    try:
        total_weighted = (means * counts).sum()
        total_count = counts.sum()
        return total_weighted / total_count
    except:
        return np.nan

def combined_std(means: pd.Series, stds: pd.Series, counts: pd.Series) -> float:
    try:
        weighted_mean = (means * counts).sum() / counts.sum()
        total_variance = (
            ((stds ** 2 + (means - weighted_mean) ** 2) * counts).sum()
            / counts.sum()
        )
        return np.sqrt(total_variance)
    except:
        return np.nan

def calculate_edge_top(delta_df, delta_name):
    if delta_df.empty:
        return "sem dados"
    delta_values = delta_df[delta_df['delta_name'] == delta_name]
    label_mean = combined_mean(delta_values['avg_delta_minutes'], delta_values['count_events']).round(1)
    label_std = combined_std(delta_values['avg_delta_minutes'], delta_values['stddev_delta_minutes'], delta_values['count_events']).round(1)
    if pd.isnull(label_mean) or pd.isnull(label_std):
        return "sem dados"
    return f"{label_mean} ± {label_std} min"

def calculate_edge_bottom(delta_df, delta_name):
    if delta_df.empty:
        return "sem dados"
    delta_values = delta_df[delta_df['delta_name'] == delta_name]
    cnt_events = delta_values['count_events'].sum()
    return "sem dados" if pd.isnull(cnt_events) else f"{cnt_events} eventos"



def render_plotly_network(json_data, delta_df):

    theme = get_streamlit_theme()

    G = nx.DiGraph()
    pos = {}

    for i, delta in enumerate(json_data):
        if delta["type"] == "between":
            start = delta["display_name_start"]
            end = delta["display_name_end"]
            delta_name = delta["delta_name"]

            if start not in pos:
                x = delta["start_x"] if i == 0 else pos.get(start, (0, 0))[0]
                y = delta["start_y"] if i == 0 else pos.get(start, (0, 0))[1]
                pos[start] = (x, y)

            if end not in pos:
                pos[end] = (delta["end_x"], delta["end_y"])

            G.add_node(start)
            G.add_node(end)
            G.add_edge(start, end,
                       top=calculate_edge_top(delta_df, delta_name),
                       bottom=calculate_edge_bottom(delta_df, delta_name))

    shapes = []
    annotations = []
    label_top = []
    label_bottom = []
    label_x = []
    label_y_top = []
    label_y_bottom = []

    node_box_width = 350   # largura proporcional ao range de x (0-2500)
    node_box_height = 40  # altura proporcional

    node_offsets = {}

    for node, (x, y) in pos.items():
        x0, x1 = x - node_box_width / 2, x + node_box_width / 2
        y0, y1 = y - node_box_height / 2, y + node_box_height / 2

        node_offsets[node] = (x0, y0, x1, y1)  # salva posição do retângulo

        shapes.append(dict(
            type="rect",
            xref="x", yref="y",
            x0=x0, y0=y0,
            x1=x1, y1=y1,
            line=dict(color=theme["secondaryColor"], width=3),
            fillcolor=theme["backgroundColor"]
        ))

        annotations.append(dict(
            x=x, y=y,
            text="<br>".join(textwrap.wrap(node, 28)),
            showarrow=False,
            font=dict(color=theme["textColor"], size=12),
            xanchor='center', yanchor='middle'
        ))

    edge_x = []
    edge_y = []
    arrow_annotations = []

    for edge in G.edges():
        source, target = edge
        x0, y0 = pos[source]
        x1, y1 = pos[target]

        # Calcula ligação de centro até borda direita/esquerda dos retângulos
        source_x1 = node_offsets[source][2]
        target_x0 = node_offsets[target][0]
        center_y_source = (node_offsets[source][1] + node_offsets[source][3]) / 2
        center_y_target = (node_offsets[target][1] + node_offsets[target][3]) / 2

        edge_x += [source_x1, target_x0, None]
        edge_y += [center_y_source, center_y_target, None]

        xm = (source_x1 + target_x0) / 2
        ym = (center_y_source + center_y_target) / 2
        label_x.append(xm)
        label_y_top.append(ym + 10)
        label_y_bottom.append(ym - 10)
        label_top.append(G.edges[edge]['top'])
        label_bottom.append(G.edges[edge]['bottom'])

        arrow_annotations.append(dict(
            x=target_x0, y=center_y_target,
            ax=source_x1, ay=center_y_source,
            xref='x', yref='y',
            axref='x', ayref='y',
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor=theme['primaryColor'],
            opacity=1
        ))

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        mode='lines',
        line=dict(width=2, color=theme['primaryColor']),
        hoverinfo='none'
    )

    label_trace_top = go.Scatter(
        x=label_x,
        y=label_y_top,
        text=label_top,
        mode="text",
        hoverinfo="none",
        textfont=dict(size=12, color="black")
    )

    label_trace_bottom = go.Scatter(
        x=label_x,
        y=label_y_bottom,
        text=label_bottom,
        mode="text",
        hoverinfo="none",
        textfont=dict(size=12, color="black")
    )

    fig = go.Figure(data=[edge_trace, label_trace_top, label_trace_bottom])
    fig.update_layout(
        showlegend=False,
        shapes=shapes,
        annotations=annotations + arrow_annotations,
        hovermode='closest',
        margin=dict(b=20, l=5, r=5, t=40),
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(showgrid=False, zeroline=False),
        height=700
    )

    return fig