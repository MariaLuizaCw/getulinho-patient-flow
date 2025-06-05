from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, Table, MetaData, select, and_
import streamlit as st
from sqlalchemy.dialects import postgresql
import os
# Conexão

# Lê as variáveis de ambiente
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST", "postgres")  
port = os.getenv("POSTGRES_PORT", "5432")
dbname = os.getenv("POSTGRES_DB")

# Monta a URL
db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

# Cria engine e metadata
engine = create_engine(db_url)
metadata = MetaData()


# Mapeamento de nomes de mês
month_name_to_number = {
    "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4,
    "Maio": 5, "Junho": 6, "Julho": 7, "Agosto": 8,
    "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12
}

# Dicionário de builders
time_filter_builders = {}

def get_last_report_time(table_name: str):
    log_table = Table("last_update_log", metadata, autoload_with=engine)
    with engine.connect() as conn:
        return conn.execute(
            select(log_table.c.report_time).where(log_table.c.table_name == table_name)
        ).scalar()
    

# Decorador para registrar funções no dicionário
def register_time_filter(name):
    def decorator(fn):
        time_filter_builders[name] = fn
        return fn
    return decorator

@register_time_filter("Últimas 4 horas")
def filter_last_4h(table, time_complement):
    table_name = table.name  # nome real da tabela como string
    target = get_last_report_time(table_name)
    if target:
        return [table.c.start_interval == target]
    return []

@register_time_filter("Últimas 24 horas")
def filter_last_24h(table, time_complement):
    table_name = table.name
    target = get_last_report_time(table_name)
    if target:
        return [table.c.start_interval == target]
    return []

@register_time_filter("Selecionar um dia")
def filter_day(table, time_complement):
    day = time_complement.get("day")

    start_dt = day
    end_dt = start_dt + timedelta(days=1)
    return [
        table.c.start_interval >= start_dt,
        table.c.start_interval < end_dt
    ]

@register_time_filter("Selecionar um mês")
def filter_month(table, time_complement):
    year = time_complement.get("year")
    month_name = time_complement.get("month")
    month = month_name_to_number.get(month_name)
    if year and month:
        start_dt = datetime(year, month, 1)
        end_dt = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
        return [table.c.start_interval >= start_dt, table.c.start_interval < end_dt]
    return []


# ---------------- Função auxiliar principal ---------------- #

def build_time_filter(table, time_range, time_complement):
    builder = time_filter_builders.get(time_range)
    if builder:
        return builder(table, time_complement)
    return []

def build_risk_filter(table, selected_risks):
    if selected_risks:
        selected_risks_upper = [r.upper() for r in selected_risks]
        return [table.c.risk_class.in_(selected_risks_upper)]
    return []


@st.cache_data()
def query_events(selected_risks, selected_times):
    time_range = selected_times.get("time_range")
    table_name = selected_times.get("time_table")
    time_complement = selected_times.get("time_complement", {})
    
    table = Table(table_name, metadata, autoload_with=engine)

    risk_filters = build_risk_filter(table, selected_risks)
    time_filters = build_time_filter(table, time_range, time_complement)

    query = select(table).where(and_(*(risk_filters + time_filters)))
    compiled_query = query.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"literal_binds": True}
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.mappings().all()

    compiled_query = query.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"literal_binds": True}
    )

    # Mostra no app
    # st.markdown("**Query SQL gerada:**")
    # st.code(str(compiled_query), language="sql")


    return [dict(row) for row in rows]