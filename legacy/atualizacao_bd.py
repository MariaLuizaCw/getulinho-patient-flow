import psycopg2
import os
import json
import requests
from datetime import datetime
import re
import psycopg2.extras
import time
from dotenv import load_dotenv, find_dotenv

# Busca variáveis de ambiente do .env
load_dotenv(find_dotenv())

base_url = json.loads(os.environ['APIS'])[0]
base_url = base_url[0]
pg_user=os.environ['POSTGRES_USER']
pg_password=os.environ['POSTGRES_PASSWORD']
pg_db=os.environ['POSTGRES_DB']
pg_host='postgres'
bearer_token = os.environ['BEARER_TOKEN']

apis = [
    f"{base_url}api/v1/acolhimento/listAll?data=04/01/2022 00:00:00",
    f"{base_url}api/ehr/v1/alergias/fromDate?data=04/01/2022 00:00:00",
    f"{base_url}api/v1/atendimento/listAll?data=04/01/2022 00:00:00",
    f"{base_url}api/v1/boletim/listAll?data=04/01/2022 00:00:00",
    f"{base_url}api/v1/classificacaorisco/listAll?data=04/01/2022 00:00:00",
    f"{base_url}api/v1/diagnostico/listAll?data=04/01/2022 00:00:00",
    f"{base_url}api/ehr/v1/episodio/fromDate?data=04/01/2022 00:00:00",
    f"{base_url}api/ehr/v1/exames/fromDate?data=01/01/2025 00:00:00",
    f"{base_url}api/v1/pacientes/fromDate?data=04/01/2022 00:00:00",
    f"{base_url}api/ehr/v1/prescricaoMedicamentos/fromDate?data=04/01/2022 00:00:00",
    f"{base_url}api/ehr/v1/prescricoes/fromDate?data=01/01/2025 00:00:00",
    f"{base_url}api/ehr/v1/dispensacaoMedicamentos/fromDate?data=01/01/2025 00:00:00",
]

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

# Função para criar tabela, se não existir
def create_table_if_not_exists(table_name, sample_data, cursor, conn):
    columns = ", ".join([f"{key} TEXT" for key in sample_data.keys() if key != '_links'])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

# Função para inserir dados no PostgreSQL
def bulk_insert_data(table_name, data_list, cursor, conn):
    if not data_list:
        return
    columns = list(data_list[0].keys())
    values = [[str(val) if val is not None else "NULL" for val in data.values()] for data in data_list]
    insert_query = f"""
    INSERT INTO {table_name} ({", ".join(columns)})
    VALUES %s
    """
    psycopg2.extras.execute_values(cursor, insert_query, values)
    conn.commit()
    print(f"Foram inseridos {len(data_list)} registros na tabela {table_name}")

# Função para extrair o nome da tabela a partir da URL
def extrair_nome_tabela(url):
    match = re.search(r'/([^/]+)/[^/]*$', url[:-19])
    if match:
        if match.group(1) == 'atendimento':
            return 'atendimentopacientes'
        if match.group(1) == 'paciente':
            return 'pacientes'
        if match.group(1) == 'exames':
            return 'exames2'
        return match.group(1)
    return "default_table"


# Função para gerar nova URL com a data atualizada
def gerar_novas_urls(url_antiga, ultima_data):
    ultima_data_obj = ultima_data.strftime("%d/%m/%Y %H:%M:%S")
    ultima_data = str(ultima_data_obj).replace('-', '/')
    url_nova = url_antiga[:-19] + ultima_data
    print('URL com data atualizada:', url_nova)
    return url_nova

# Função que obtém a última data da tabela `ultima_atualizacao`
def obter_ultima_data(conn, tabela, data_url):
    try:
        cursor = conn.cursor()
        # Consultar a tabela `ultima_atualizacao` para obter a última data para a tabela específica
        query = "SELECT ultima_datahora FROM ultima_atualizacao WHERE nome_tabela = %s"
        cursor.execute(query, (tabela.lower(),))
        resultado = cursor.fetchone()

        if resultado and resultado[0]:
            ultima_data = resultado[0]  # Data encontrada na tabela
            print(f'Última data obtida da tabela `ultima_atualizacao` para {tabela}: {ultima_data}')
            return datetime.strptime(ultima_data, "%Y-%m-%d %H:%M:%S")
        else:
            # Se a tabela não estiver registrada em `ultima_atualizacao`, usar a data inicial da URL
            print(f'Tabela {tabela} não registrada em `ultima_atualizacao`. Usando data da URL.')
            return data_url
    except Exception as e:
        print(f"Erro ao obter a última data de `ultima_atualizacao`: {e}")
        return None

# Função para atualizar a tabela `ultima_atualizacao` após o loop
def atualizar_ultima_atualizacao(conn, tabela, nova_datahora):
    try:
        # Atualizar a data se a tabela já existir
        query_update = """
        UPDATE ultima_atualizacao
        SET ultima_datahora = %s
        WHERE nome_tabela = %s
        """
        cursor.execute(query_update, (nova_datahora, tabela.lower()))
        
        conn.commit()
        print(f"Tabela `ultima_atualizacao` atualizada: {tabela} -> {nova_datahora}")
    except Exception as e:
        print(f"Erro ao atualizar `ultima_atualizacao`: {e}")

# Função para buscar e inserir dados da API
def fetch_and_insert_data(url, cursor, conn, nome_tabela, bearer_token):
    headers = {"Authorization": f"Bearer {bearer_token}"}
    while True: 
        data_url = url[-19:]  # Data inicial da URL
        ultima_data = obter_ultima_data(conn, nome_tabela, data_url)
        nova_url = gerar_novas_urls(url, ultima_data)
        print(nova_url)
        response = requests.get(nova_url, headers=headers)
        response.raise_for_status()
        print('Requisição à API realizada')
        data = response.json()
        if not isinstance(data, list):
            data = [data]  # Certifique-se de que os dados estejam em formato de lista
        if data:
            # Criar tabela na primeira vez
            create_table_if_not_exists(nome_tabela, data[0], cursor, conn)
            # Inserir dados
            bulk_insert_data(nome_tabela, data, cursor, conn)
            print('Uma leva de ingestão foi realizada')
            # Atualizar a tabela `ultima_atualizacao` com a última data do JSON
            ultima_data_json = data[-1].get('dataHora')  # Acessar a última entrada
            if ultima_data_json:
                atualizar_ultima_atualizacao(conn, nome_tabela, ultima_data_json)
            time.sleep(20)
        if len(data) < 1000:
            print(f'Tabela {nome_tabela} atualizada.')
            break


#função que remove duplicatas do bd
def remove_duplicatas(str_lista_colunas,nome_tabela,cursor, conn):
    if str_lista_colunas==None or nome_tabela==None:
        raise TypeError('Lista de colunas ou lista de tabelas são igual a None')
    try:
        #query SQL que faz remoção de duplicata
        remove_duplic= f"""WITH duplicatas AS (
            SELECT ctid,{str_lista_colunas},
                ROW_NUMBER() OVER (PARTITION BY {str_lista_colunas} ORDER BY ctid) AS rn
            FROM {nome_tabela}
            )
            DELETE FROM {nome_tabela}
            WHERE ctid IN (
            SELECT ctid
            FROM duplicatas
            WHERE rn > 1);"""
        cursor.execute(remove_duplic)
        conn.commit()
        return True
    except Exception as e:
        print(f"Erro ao remover duplicata na tabela {nome_tabela}: {e}")
        return None
    
def lista_tabelas(cursor,conn):
    try:
        #query que retorna todos os nomes de tabelas
        query_tabelas = """SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public' 
                        ORDER BY table_name;"""
        cursor.execute(query_tabelas)
        tabelas = cursor.fetchall()
        lista_tabelas = [tabela[0] for tabela in tabelas]
        return lista_tabelas
    except Exception as e:
        print(f"Erro ao obter lista de tabelas: {e}")
        return None
    
def lista_colunas(nome_tabela,cursor,conn):
    try: 
        #query que retorna lista com todas as colunas de uma tabela
        query_colunas = f"""SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = '{nome_tabela}'
                        ORDER BY ordinal_position;"""
        cursor.execute(query_colunas)
        colunas = cursor.fetchall()
        str_lista_colunas = ', '.join([coluna[0] for coluna in colunas])
        return str_lista_colunas
    except Exception as e:
        print(f"Erro ao obter lista de colunas da tabela {nome_tabela}: {e}")
        return None


# Conectar ao banco de dados
conn = conectar_postgres()
if conn:
    cursor = conn.cursor()

#loop de atualização do bd 
for url in apis:
    try:
        nome=extrair_nome_tabela(url)
        print('Iniciando atualização da tabela:', nome)
        fetch_and_insert_data(url, cursor, conn, nome, bearer_token)
    
    
    except Exception as e:  # Captura o erro e armazena detalhes
        print(f'Erro ao processar {url}:', e)  # Exibe o erro para diagnóstico
        continue 

#remoção das duplicatas 
for tabela in lista_tabelas(cursor, conn):
    if remove_duplicatas(lista_colunas(tabela,cursor,conn),tabela,cursor,conn):
        print(f'Duplicatas removidas da tabela {tabela}')
    else: break

# Fechar a conexão no final
cursor.close()
conn.close()
print('Conexão com o BD Getulinho fechada')
