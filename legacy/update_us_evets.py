#imports + config
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

pg_user=os.environ['POSTGRES_USER']
pg_password=os.environ['POSTGRES_PASSWORD']
pg_db=os.environ['POSTGRES_DB']
pg_host='postgres'
bearer_token = os.environ['BEARER_TOKEN']

def conectar_postgres():
    try:
        conn = psycopg2.connect(
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port="5432" 
        )
        print("Conexão ao BD Getulinho estabelecida.")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao BD Getulinho: {e}")
        return None

conn = conectar_postgres()
if conn:
    cursor = conn.cursor()
agora = datetime.now()
hora_truncada = agora.replace(minute=0, second=0, microsecond=0)
variavel_tempo = hora_truncada.strftime('%Y-%m-%d %H:%M:%S')

# Funções auxiliares
def intervalo(timestamp_str, interv=1):
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    new_timestamp = timestamp - timedelta(hours=interv)
    return new_timestamp.strftime('%Y-%m-%d %H:%M:%S')

def puxar_dados(conn, nome_tabela, colunas, coluna_datahora, variavel_tempo, interv=1):
    colunas_str = ', '.join(colunas)
    variavel_tempo_inicial = intervalo(variavel_tempo, interv)
    query = f"""
    SELECT {colunas_str}
    FROM {nome_tabela}
    WHERE {coluna_datahora} BETWEEN %s AND %s
    ORDER BY {coluna_datahora} ASC;
    """
    cursor.execute(query, (variavel_tempo_inicial, variavel_tempo))
    resultados = cursor.fetchall()
    return pd.DataFrame(resultados, columns=colunas)

def buscar_por_lista(conn, tabela, colunas, coluna_filtro, valores):
    query = f"""
        SELECT {colunas}
        FROM "{tabela}"
        WHERE "{coluna_filtro}" = ANY(%s)
    """
    cursor.execute(query, (valores,))
    resultados = cursor.fetchall()
    return pd.DataFrame(resultados, columns=colunas.split(','))

def truncar_hora(df, col_data, nova_col='datahora_trunc'):
    df[nova_col] = df[col_data].dt.floor('h')
    return df

# Função para criar partição caso ainda não exista
def criar_particao_se_necessario(conn, datahora_truncada):
    inicio = datahora_truncada
    fim = inicio + timedelta(hours=1)
    nome_particao = f"us_events_{inicio.strftime('%Y%m%d%H')}"
    cursor.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class WHERE relname = '{nome_particao}'
            ) THEN
                EXECUTE $create$
                    CREATE TABLE IF NOT EXISTS {nome_particao} PARTITION OF us_events
                    FOR VALUES FROM ('{inicio}') TO ('{fim}');
                $create$;
            END IF;
        END
        $$;
    """)
    conn.commit()

todos_eventos = []

#Evento: acolhimento obs: só se tem pacienteid após o registro, então ocorre perdas
df = puxar_dados(conn, 'acolhimento', ['nome', 'data'], 'data', variavel_tempo).drop_duplicates('nome')
lista_nome= df['nome'].tolist()
df_pacientes= buscar_por_lista(conn, 'pacientes', 'id,nome', 'nome', lista_nome).drop_duplicates()
df= df.merge(df_pacientes, on='nome', how='inner')
df=df.rename(columns={'data': 'datahora','id':'pacienteid'})
df= df[['pacienteid', 'datahora']]
df['evento'] = 'acolhimento'
df['datahora'] = pd.to_datetime(df['datahora'])
df=truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Evento: registro
df = puxar_dados(conn, 'boletim', ['pacienteid', 'dataentrada', 'tipoatendimento'], 'dataentrada', variavel_tempo)
df = df[df['tipoatendimento'] == 'EMERGENCIA   '].drop('tipoatendimento', axis=1).drop_duplicates()
df = df.rename(columns={'dataentrada': 'datahora'})
df['evento'] = 'registro'
df['datahora'] = pd.to_datetime(df['datahora'])
df = truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Evento: inicio_classificacao
df = puxar_dados(conn, 'classificacaorisco', ['pacienteid', 'datainicio'], 'datainicio', variavel_tempo).drop_duplicates()
df = df.rename(columns={'datainicio': 'datahora'})
df['evento'] = 'inicio_classific'
df['datahora'] = pd.to_datetime(df['datahora'])
df = truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Evento: fim_classificacao
df = puxar_dados(conn, 'classificacaorisco', ['pacienteid', 'datafim'], 'datafim', variavel_tempo).drop_duplicates()
df = df.rename(columns={'datafim': 'datahora'})
df['evento'] = 'fim_classific'
df['datahora'] = pd.to_datetime(df['datahora'])
df = truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Evento: inicio_consulta1
df = puxar_dados(conn, 'atendimentopacientes', ['pacienteid', 'dthrinicio', 'tipoatendimento'], 'dthrinicio', variavel_tempo)
df = df[df['tipoatendimento'] == 'ATENDIMENTO'].drop('tipoatendimento', axis=1).drop_duplicates()
df = df.rename(columns={'dthrinicio': 'datahora'})
df['evento'] = 'inicio_consulta1'
df['datahora'] = pd.to_datetime(df['datahora'])
df = truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Evento: inicio_consulta2
df = puxar_dados(conn, 'atendimentopacientes', ['pacienteid', 'dthrinicio', 'tipoatendimento'], 'dthrinicio', variavel_tempo)
df = df[df['tipoatendimento'] == 'REAVALIACAO'].drop('tipoatendimento', axis=1).drop_duplicates()
df = df.rename(columns={'dthrinicio': 'datahora'})
df['evento'] = 'inicio_consulta2'
df['datahora'] = pd.to_datetime(df['datahora'])
df = truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Evento: fim_consulta1
df = puxar_dados(conn, 'atendimentopacientes', ['pacienteid', 'dthrfim', 'tipoatendimento'], 'dthrfim', variavel_tempo)
df = df[df['tipoatendimento'] == 'ATENDIMENTO'].drop('tipoatendimento', axis=1).drop_duplicates()
df = df.rename(columns={'dthrfim': 'datahora'})
df['evento'] = 'fim_consulta1'
df['datahora'] = pd.to_datetime(df['datahora'])
df = truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Evento: fim_consulta2
df = puxar_dados(conn, 'atendimentopacientes', ['pacienteid', 'dthrfim', 'tipoatendimento'], 'dthrfim', variavel_tempo)
df = df[df['tipoatendimento'] == 'REAVALIACAO'].drop('tipoatendimento', axis=1).drop_duplicates()
df = df.rename(columns={'dthrfim': 'datahora'})
df['evento'] = 'fim_consulta2'
df['datahora'] = pd.to_datetime(df['datahora'])
df = truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Evento: inicio_internacao1 - internação após primeiro atendimento 
df = puxar_dados(conn, 'boletim', ['pacienteid', 'datainternacao', 'tipoatendimento'], 'datainternacao', variavel_tempo)
df_internacao1_raw = df[df['tipoatendimento'] == 'EMERGENCIA   '].drop('tipoatendimento', axis=1).drop_duplicates()
df_consulta= puxar_dados(conn, 'atendimentopacientes', ['pacienteid', 'tipoatendimento'], 'dthrfim', variavel_tempo, 24).drop_duplicates()
df_merged = pd.merge(df_internacao1_raw, df_consulta, on='pacienteid', how='inner')
#pegar apenas os ids que tiveram apenas consulta 
contagem = df_merged.groupby('pacienteid').size() #contar a ocorrencia de cada id no df merged 
ids_unicos = contagem[contagem == 1].index #pegar ids que aparecem 1 vez (n teve reavaliação)
df_merged_clean = df_merged[df_merged['pacienteid'].isin(ids_unicos)]
df_merged_clean = df_merged_clean[df_merged_clean['tipoatendimento'] == 'ATENDIMENTO'].drop('tipoatendimento', axis=1)
df_merged_clean = df_merged_clean.rename(columns={'datainternacao': 'datahora'})
df_merged_clean['evento'] = 'inicio_internacao1'
df_merged_clean['datahora'] = pd.to_datetime(df_merged_clean['datahora'])
df_merged_clean = truncar_hora(df_merged_clean, 'datahora')
todos_eventos.append(df_merged_clean)

# Evento: inicio_internacao2 - internação após reavaliação 
df = puxar_dados(conn, 'boletim', ['pacienteid', 'datainternacao', 'tipoatendimento'], 'datainternacao', variavel_tempo)
df_internacao2_raw = df[df['tipoatendimento'] == 'EMERGENCIA   '].drop('tipoatendimento', axis=1).drop_duplicates()
df_consulta= puxar_dados(conn, 'atendimentopacientes', ['pacienteid', 'tipoatendimento'], 'dthrfim', variavel_tempo, 24).drop_duplicates()
df_merged = pd.merge(df_internacao2_raw, df_consulta, on='pacienteid', how='inner')
#pegar apenas os ids que tiveram apenas consulta 
contagem = df_merged.groupby('pacienteid').size() #contar a ocorrencia de cada id no df merged 
ids_unicos = contagem[contagem == 2].index #pegar ids que aparecem 2 vezes (consulta + reavaliação)
df_merged_clean = df_merged[df_merged['pacienteid'].isin(ids_unicos)]
df_merged_clean = df_merged_clean[df_merged_clean['tipoatendimento'] == 'REAVALIACAO'].drop('tipoatendimento', axis=1)
df_merged_clean = df_merged_clean.rename(columns={'datainternacao': 'datahora'})
df_merged_clean['evento'] = 'inicio_internacao1'
df_merged_clean['datahora'] = pd.to_datetime(df_merged_clean['datahora'])
df_merged_clean = truncar_hora(df_merged_clean, 'datahora')
todos_eventos.append(df_merged_clean)

# Evento: exame
df = puxar_dados(conn, 'exames2', ['pacienteid', 'datarealizacao', 'status'], 'datarealizacao', variavel_tempo)
df = df[df['status'] == 'REALIZADO'].drop('status', axis=1).drop_duplicates()
df = df.rename(columns={'datarealizacao': 'datahora'})
df['evento'] = 'exame'
df['datahora'] = pd.to_datetime(df['datahora'])
df = truncar_hora(df, 'datahora')
todos_eventos.append(df)

# Desfechos classificados como alta
df_desfechos = puxar_dados(conn, 'episodio', ['pacienteid', 'dataalta', 'motivoalta', 'tipoatendimento'], 'dataalta', variavel_tempo).drop_duplicates()
df_desfechos = df_desfechos[df_desfechos['tipoatendimento'] == 'EMERGENCIA   '].drop('tipoatendimento', axis=1)
alta_obito=['OBITO',"COM DECLARAÇÃO DE ÓBITO FORNECIDA PELO MÉDICO ASSISTENTE","COM DECLARAÇÃO DE ÓBITO FORNECIDA PELO INSTITUTO MÉDICO LEGAL - IML"]
alta_melhorado=['ALTA MELHORADO','ALTA MEDICA', "ALTA DA UNIDADE", "ALTA CURADO"]
alta_evasão=["ALTA POR EVASÃO","NÃO RESPONDEU AO CHAMADO"]
alta_revelia=["ALTA POR REVELIA","ALTA POR OUTROS MOTIVOS","ALTA A PEDIDO","ALTA COM PREVISÃO DE RETORNO PARA ACOMPANHAMENTO DO PACIENTE","ALTA COM RESERVA"]
alta_transferencia=["TRANSFERENCIA","ALTA POR TRANSFERENCIA EXTERNA","TRANSFERIDO PARA OUTRO ESTABELECIMENTO"]
#Evento: óbito após internação
df_obito3=df_desfechos[df_desfechos['motivoalta'].isin(alta_obito)]
df_obito3 = df_obito3.drop('motivoalta', axis=1)
df_obito3 = df_obito3.rename(columns={'dataalta': 'datahora'})
df_obito3['evento'] = 'obito3'
df_obito3['datahora'] = pd.to_datetime(df_obito3['datahora'])
df_obito3 = truncar_hora(df_obito3, 'datahora')
todos_eventos.append(df_obito3)
#Evento: transferência após internação
df_transf=df_desfechos[df_desfechos['motivoalta'].isin(alta_transferencia)]
df_transf = df_transf.drop('motivoalta', axis=1)
df_transf = df_transf.rename(columns={'dataalta': 'datahora'})
df_transf['evento'] = 'transferencia'
df_transf['datahora'] = pd.to_datetime(df_transf['datahora'])
df_transf = truncar_hora(df_transf, 'datahora')
todos_eventos.append(df_transf)
#Evento: alta hospitalar após internação
#melhorado
df_alta_melhorado=df_desfechos[df_desfechos['motivoalta'].isin(alta_melhorado)]
df_alta_melhorado = df_alta_melhorado.drop('motivoalta', axis=1)
df_alta_melhorado = df_alta_melhorado.rename(columns={'dataalta': 'datahora'})
df_alta_melhorado['evento'] = 'alta_melhorado'
df_alta_melhorado['datahora'] = pd.to_datetime(df_alta_melhorado['datahora'])
df_alta_melhorado = truncar_hora(df_alta_melhorado, 'datahora')
todos_eventos.append(df_alta_melhorado)
#evasão
df_alta_evasao=df_desfechos[df_desfechos['motivoalta'].isin(alta_evasão)]
df_alta_evasao = df_alta_evasao.drop('motivoalta', axis=1)
df_alta_evasao = df_alta_evasao.rename(columns={'dataalta': 'datahora'})
df_alta_evasao['evento'] = 'alta_evasão'
df_alta_evasao['datahora'] = pd.to_datetime(df_alta_evasao['datahora'])
df_alta_evasao = truncar_hora(df_alta_evasao, 'datahora')
todos_eventos.append(df_alta_evasao)
#revelia
df_alta_revelia=df_desfechos[df_desfechos['motivoalta'].isin(alta_revelia)]
df_alta_revelia = df_alta_revelia.drop('motivoalta', axis=1)
df_alta_revelia = df_alta_revelia.rename(columns={'dataalta': 'datahora'})
df_alta_revelia['evento'] = 'alta_revelia'
df_alta_revelia['datahora'] = pd.to_datetime(df_alta_revelia['datahora'])
df_alta_revelia = truncar_hora(df_alta_revelia, 'datahora')
todos_eventos.append(df_alta_revelia)

# Unir tudo no final
df_final = pd.concat(todos_eventos).drop_duplicates()
df_final['evento']=df_final['evento'].astype('str')
df_final['pacienteid']=df_final['pacienteid'].astype('str')

for hora in df_final["datahora_trunc"].unique():
        criar_particao_se_necessario(conn, hora)

# Inserir na tabela
for _, row in df_final.iterrows():
    cursor.execute("""
        INSERT INTO us_events (pacienteid, evento, datahora, datahora_trunc)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (row['pacienteid'], row['evento'], row['datahora'], row['datahora_trunc']))
conn.commit()

# Fecha conexão com o banco
cursor.close()
conn.close()