# Standard library
import os
import json
import logging

# Third-party libraries
import numpy as np
from sqlalchemy import func, and_, MetaData, Table, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

# Project-specific imports
from utils.database import create_engine_postgres


# Configuração de logger (caso ainda não tenha configurado)
logger = logging.getLogger(__name__)


class DeltaBase:
    def __init__(self, current_hour, source_schema, source_table_name):
        self.current_hour = current_hour
        self.source_schema = source_schema
        self.source_table_name = source_table_name
        self.engine = create_engine_postgres()  # Assume que create_engine_postgres é a função para criar a conexão
        self.metadata = MetaData()

    def load_sql_query(self, file_path='delta_calculators/query_delta.sql'):
        """
        Função para ler o conteúdo de uma consulta SQL de um arquivo.
        """
        # Usar o AIRFLOW_HOME ou o caminho absoluto para garantir que o arquivo seja encontrado
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        file_path = os.path.join(airflow_home, 'modules', file_path)  # Ajuste do caminho conforme necessário

        with open(file_path, 'r') as file:
            return file.read()

    def get_distinct_spare_values(self, start_event, end_event, start_interval, end_interval):
        session = sessionmaker(bind=self.engine)()
        source_table = Table(self.source_table_name, self.metadata, autoload_with=self.engine, schema=self.source_schema)

        # Filtros de intervalo de tempo
        time_filter = f"AND real_date BETWEEN '{start_interval}'::timestamp AND '{end_interval}'::timestamp"

        # Inicializando um dicionário para armazenar os spare1 e spare2 de ambos os eventos
        spare_columns = {
            'spare1_start': [],
            'spare2_start': [],
            'spare1_end': [],
            'spare2_end': []
        }

        # Consultas para obter os valores distintos de spare1 e spare2 para o start_event
        distinct_spare1_start_query = session.execute(
            text(f"""
                SELECT DISTINCT spare1, spare2
                FROM {self.source_schema}.{self.source_table_name}
                WHERE event_name = :start_event {time_filter}
            """), {'start_event': start_event}).fetchall()

        # Consultas para obter os valores distintos de spare1 e spare2 para o end_event
        distinct_spare1_end_query = session.execute(
            text(f"""
                SELECT DISTINCT spare1, spare2
                FROM {self.source_schema}.{self.source_table_name}
                WHERE event_name = :end_event {time_filter}
            """), {'end_event': end_event}).fetchall()

        # Preenchendo os valores de spare1 e spare2 para os dois eventos
        spare_columns['spare1_start'] = [row[0] for row in distinct_spare1_start_query if row[0] is not None]
        spare_columns['spare2_start'] = [row[1] for row in distinct_spare1_start_query if row[1] is not None]
        
        spare_columns['spare1_end'] = [row[0] for row in distinct_spare1_end_query if row[0] is not None]
        spare_columns['spare2_end'] = [row[1] for row in distinct_spare1_end_query if row[1] is not None]

        session.close()
        logger.info(f"Spare Columns: {spare_columns}") 
        return spare_columns
    def execute_delta_query(self, start_interval, end_interval, sequence, spare_columns):
        """
        Função para executar a consulta SQL, lendo o arquivo .sql e substituindo os parâmetros.
        """
        query = self.load_sql_query()

        # Substituir os parâmetros da consulta SQL
        query = query.replace(':start_interval', start_interval.strftime('%Y-%m-%d %H:%M:%S'))
        query = query.replace(':end_interval', end_interval.strftime('%Y-%m-%d %H:%M:%S'))
        query = query.replace(':delta_name', sequence["delta_name"])
        query = query.replace(':start_event', sequence["start_event"])
        query = query.replace(':end_event', sequence["end_event"])
        lag_value = sequence.get("lag", 1)  # pega sequence["lag"] ou usa 1 como padrão
        query = query.replace(':lag_n', str(lag_value))


        for spare_name, spare_values in spare_columns.items():
            # Gerar a parte de contagem
            spare_part = ", ".join([f"COUNT(CASE WHEN {spare_name} = '{value}' THEN 1 END) AS \"{value}_count\"" 
                                    for value in spare_values])

            # Gerar a parte JSON com contagens
            json_part = "jsonb_build_object(" + ", ".join(
                [f"'{value}' , COUNT(CASE WHEN {spare_name} = '{value}' THEN 1 END)" 
                for value in spare_values]
            ) + ")"
            query = query.replace(f':{spare_name}', json_part)

        logger.info(f"Executing SQL query: {query}") 
        # Executar a consulta usando SQLAlchemy
        session = sessionmaker(bind=self.engine)()
        result = session.execute(text(query)).fetchall()
        session.close()
        return result

    def insert_results_into_db(self, table_name, results):
        """
        Função para inserir os resultados calculados em uma tabela dinâmica.
        """
        session = sessionmaker(bind=self.engine)()

        # Carregar a tabela de saída (dado o nome da tabela)
        output_table = Table(table_name, self.metadata, autoload_with=self.engine, schema=self.source_schema)

        for result in results:
            # Convertendo o resultado (Row) para dicionário
            result_dict = dict(result)  # Converte a Row para dicionário

            # Ajuste dos dados antes de inserir
            insert_data = {
                "delta_name": result_dict["delta_name"],
                "avg_delta_minutes": result_dict["avg_delta_minutes"],
                "stddev_delta_minutes": result_dict["stddev_delta_minutes"],
                "min_delta_minutes": result_dict["min_delta_minutes"],
                "max_delta_minutes": result_dict["max_delta_minutes"],
                "p25_delta_minutes": result_dict["p25_delta_minutes"],
                "p50_delta_minutes": result_dict["p50_delta_minutes"],
                "p75_delta_minutes": result_dict["p75_delta_minutes"],
                "count_events": result_dict["count_events"],
                "risk_class": result_dict["risk_class"],
                "spare1_start_count_json": json.dumps(result_dict["spare1_start"]) if result_dict.get("spare1_start") else None,
                "spare2_start_count_json": json.dumps(result_dict["spare2_start"]) if result_dict.get("spare2_start") else None,
                "spare1_end_count_json": json.dumps(result_dict["spare1_end"]) if result_dict.get("spare1_end") else None,
                "spare2_end_count_json": json.dumps(result_dict["spare2_end"]) if result_dict.get("spare2_end") else None,
                "start_interval": result_dict["start_interval"],  # Certifique-se de que a data está no formato correto
                "end_interval": result_dict["end_interval"]  # Certifique-se de que a data está no formato correto
            }

   
            insert_stmt = insert(output_table).values(insert_data)
            insert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["delta_name", "start_interval", "end_interval", "risk_class"],  # Definindo as colunas de conflito
                set_={
                    "delta_name": insert_stmt.excluded["delta_name"],  # Atualiza delta_name com o novo valor
                    "avg_delta_minutes": insert_stmt.excluded["avg_delta_minutes"],  # Atualiza avg_delta_minutes
                    "stddev_delta_minutes": insert_stmt.excluded["stddev_delta_minutes"],  # Atualiza stddev_delta_minutes
                    "min_delta_minutes": insert_stmt.excluded["min_delta_minutes"],  # Atualiza min_delta_minutes
                    "max_delta_minutes": insert_stmt.excluded["max_delta_minutes"],  # Atualiza max_delta_minutes
                    "p25_delta_minutes": insert_stmt.excluded["p25_delta_minutes"],  # Atualiza p25_delta_minutes
                    "p50_delta_minutes": insert_stmt.excluded["p50_delta_minutes"],  # Atualiza p50_delta_minutes
                    "p75_delta_minutes": insert_stmt.excluded["p75_delta_minutes"],  # Atualiza p75_delta_minutes
                    "count_events": insert_stmt.excluded["count_events"],  # Atualiza count_events
                    "risk_class": insert_stmt.excluded["risk_class"],  # Atualiza risk_class
                    "spare1_start_count_json": insert_stmt.excluded["spare1_start_count_json"],  # Atualiza spare1_start_count_json
                    "spare2_start_count_json": insert_stmt.excluded["spare2_start_count_json"],  # Atualiza spare2_start_count_json
                    "spare1_end_count_json": insert_stmt.excluded["spare1_end_count_json"],  # Atualiza spare1_end_count_json
                    "spare2_end_count_json": insert_stmt.excluded["spare2_end_count_json"],  # Atualiza spare2_end_count_json
                    "start_interval": insert_stmt.excluded["start_interval"],  # Atualiza start_interval
                    "end_interval": insert_stmt.excluded["end_interval"],  # Atualiza end_interval
                }
            )
            
            # Executar a consulta
            session.execute(insert_stmt)

        # Commit da transação
        session.commit()
        session.close()


        logger.info(f"Dados inseridos na tabela {table_name} com sucesso!")

        