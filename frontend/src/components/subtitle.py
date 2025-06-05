from utils.theme import get_streamlit_theme
import streamlit as st

def render_subtitle(delta_stats):
    theme = get_streamlit_theme()

    primaryColor = theme['primaryColor']
    if delta_stats:
        start = delta_stats[0]['start_interval']
        end = delta_stats[0]['end_interval']
        
        st.markdown(
        f'Dados entre <span style="color:{primaryColor}">{start}</span> e <span style="color:{primaryColor}">{end}</span>',
        unsafe_allow_html=True
        )
    else:
        st.markdown(
            f'  <span style="color:{primaryColor}"> Dados não encontrados</span>',
            unsafe_allow_html=True
        )