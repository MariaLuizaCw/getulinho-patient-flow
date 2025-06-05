
import streamlit as st
from datetime import datetime

def render_risk_classification():
    risk_classes = ["Azul", "Verde", "Amarelo", "Vermelho", "Indefinido"]
    selected_risks = st.multiselect(
        "Classificação de Risco:", options=risk_classes, default=risk_classes
    )
    
    return selected_risks


def render_specific_day():
    specific_day = st.date_input("Data selecionada:", value=datetime.now())
    return {"day": specific_day}

def render_specific_month():
    col1, col2 = st.columns(2)
    with col1:
        specific_year = st.number_input(
            "Ano Específico:", min_value=2021, max_value=datetime.now().year, 
            step=1, format="%d", value=2023
        )
    with col2:
        specific_month = st.selectbox(
            "Mês Específico:", 
            options=[
                "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", 
                "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
            ]
        )
    return {"year": specific_year, "month": specific_month}

def render_specific_year():
    specific_year = st.number_input(
        "Ano Específico:", min_value=2021, max_value=datetime.now().year, 
        step=1, format="%d", value=2023
    )
    return {"year": specific_year}

def render_time_filters(time_options):
    selected_times = {}

    time_conditioned = st.selectbox(
        "Intervalo de Tempo",
        options=time_options.keys(),
        index=1
    )

    selected_times['time_range'] = time_conditioned
    selected_times['time_table'] = time_options[time_conditioned]
    selected_times['time_complement'] = {}

    if time_conditioned == 'Selecionar um dia':
        selected_times['time_complement'] = render_specific_day()
    elif time_conditioned == 'Selecionar um mês':
        selected_times['time_complement'] = render_specific_month()
    elif time_conditioned == 'Selecionar um ano':
        selected_times['time_complement'] = render_specific_year()

    return selected_times
