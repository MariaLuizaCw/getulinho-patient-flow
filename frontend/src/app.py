import streamlit as st
from components.filters import render_risk_classification, render_time_filters
from components.subtitle import render_subtitle
import streamlit.components.v1 as components
from components.extract_info import query_events
import json
import pandas as pd
import time
from components.flow import render_from_json
from streamlit_javascript import st_javascript

time_options = {
   "Últimas 4 horas": "events_last_4h", 
   "Últimas 24 horas": "events_last_24h", 
   "Selecionar um dia": "events_day", 
   "Selecionar um mês": "events_month"
}

def main():
   st.set_page_config(layout = 'wide')



   st.title("Lasos FluxoSaúde: Painel de Monitoramente Inteligente do Fluxo Hospitalar")
   subtitle = st.container()


   # Layout for filters
   col1, col2 = st.columns(2)

   with col1:
      selected_risks = render_risk_classification()

   with col2:
      selected_times = render_time_filters(time_options)

 
   delta_stats = query_events(selected_risks, selected_times)
   with subtitle:
      render_subtitle(delta_stats)

   ## Render Flow
   delta_df = pd.DataFrame(delta_stats)
   with open("/common_settings/delta_sequences.json") as f:
       json_data = json.load(f)

   flow_html = render_from_json(json_data, delta_df)
   st.components.v1.html(flow_html, height=600)

if __name__ == "__main__":
   main()