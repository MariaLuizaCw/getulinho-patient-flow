from pyvis.network import Network
import tempfile
import os
import textwrap
import streamlit.components.v1 as components
import toml
import streamlit as st
import numpy as np
import pandas as pd
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

def calculate_edge_top(delta_df, delta_name, method='mean'):
    if delta_df.empty:
        return "sem dados"

    if method == 'mean':
        delta_values = delta_df[delta_df['delta_name'] == delta_name]

        label_mean = combined_mean(delta_values['avg_delta_minutes'], delta_values['count_events']).round(1)
        label_std = combined_std(delta_values['avg_delta_minutes'], delta_values['stddev_delta_minutes'], delta_values['count_events']).round(1)
        
        if pd.isnull(label_mean) or pd.isnull(label_std):
            label="sem dados"
        else:
            label = f"{label_mean} ± {label_std} min"
          
        return label
def calculate_edge_bottom(delta_df, delta_name, method='mean'):
    if delta_df.empty:
        return "sem dados"

    if method == 'mean':
        delta_values = delta_df[delta_df['delta_name'] == delta_name]
        cnt_events = delta_values['count_events'].sum()
        if pd.isnull(cnt_events):
            label="sem dados"
        else:
            label = f"{cnt_events} eventos"
          
        return label

def render_from_json(json_data, delta_df):
    net = Network(height="500px", width="100%", directed=True)
    theme = get_streamlit_theme()

    # Desativa física para respeitar posições fixas
    net.set_options("""
    var options = {
      "physics": {
        "enabled": false
      }
    }
    """)
    text_size = 20
    default_node_style = {
        "shape": "box",
        "widthConstraint": 150,
        "heightConstraint": {"minimum": 80},
        "color": {
            "background": "white",
            "border": theme['secondaryColor'],
            "highlight": {
                "background": theme['secondaryColorLight'],
                "border": theme['secondaryColor']
            },
        },
        "font": {
            "color": "black",
            "size": text_size
        },
        "fixed": True,
        "physics": False
    }


    node_positions = {}
    display_name_map = {}

    char_per_line = 15
    for delta in json_data:
        if delta["type"] == "between":
            start_label = delta["display_name_start"]
            end_label = delta["display_name_end"]
            delta_name = delta['delta_name']

            if start_label not in node_positions:
                x, y = delta.get("start_x", 0), delta.get("start_y", 0)
                wrapped = " ".join(textwrap.wrap(start_label, char_per_line))
                net.add_node(
                    start_label, 
                    label=wrapped, 
                    x=x, 
                    y=y, 
                    **default_node_style
                )
                node_positions[start_label] = (x, y)

            if end_label not in node_positions:
                x, y = delta["end_x"], delta["end_y"]
                wrapped = " ".join(textwrap.wrap(end_label, char_per_line))
                net.add_node(
                    end_label, 
                    label=wrapped, 
                    x=x, 
                    y=y, 
                    **default_node_style
                )
                node_positions[end_label] = (x, y)

            net.add_edge(
                start_label,
                end_label,
                label=calculate_edge_top(delta_df, delta_name) + '\n \n',
                color=theme['primaryColor'],
                smooth={"type": "cubicBezier", "roundness": 0.2, "forceDirection": "horizontal"},
                    font={
                    "align": "top",
                    "size": text_size,
                    "face": "arial",
                    "color": 'black',
                    "bold": True  # opcional, pode remover se não quiser negrito
                }
            )
            net.add_edge(
                start_label,
                end_label,
                label= '\n' + calculate_edge_bottom(delta_df, delta_name),
                color="rgba(0,0,0,0)",  # torna a linha invisível
                smooth={"type": "cubicBezier", "roundness": -0.2, "forceDirection": "horizontal"},
                    font={
                    "align": "bottom",
                    "size": text_size,
                    "face": "arial",
                    "color": 'black',
                    "bold": True  # opcional, pode remover se não quiser negrito
                }
            )

        elif delta["type"] == "box":
            label = delta["display_name"]
            delta_name = delta["delta_name"]
            x, y = node_positions.get(label, (0, 0))
            wrapped = "<br>".join(textwrap.wrap(label, char_per_line))

            if label not in node_positions:
                # Adiciona o nó principal
                net.add_node(label, label=wrapped, x=x, y=y, fixed=True, physics=False, shape="box",
                            widthConstraint=node_width)
                node_positions[label] = (x, y)


            net.add_node(
                f"{label}_delta_top", 
                label=calculate_edge_top(delta_df, delta_name), 
                x=x, 
                y=y - 65, 
                fixed=True, 
                physics=False, 
                shape="text",
                font={
                    "size": text_size, 
                    "bold": True,
                    "color": 'black', 
                }
            )
            
            net.add_node(
                f"{label}_delta_bottom", 
                label=calculate_edge_bottom(delta_df, delta_name), 
                x=x, 
                y=y + 65, 
                fixed=True, 
                physics=False, 
                shape="text",
                font={
                    "size": text_size, 
                    "bold": True,
                    "color": 'black', 
                }
            )

    # Escreve o HTML em arquivo temporário e retorna o conteúdo como string
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.write_html(tmp_file.name)
        path = tmp_file.name

    with open(path, "r", encoding="utf-8") as f:
        html_content = f.read()

    os.remove(path)

    return html_content