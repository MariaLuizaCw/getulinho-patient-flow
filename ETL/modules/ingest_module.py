# Standard library
import os
import json
import logging
from datetime import datetime
from urllib.parse import urlencode

# Third-party libraries
import requests
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import Table, MetaData, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Project-specific imports
from utils.database import update_last_datetime, create_engine_postgres


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

class APIFetcher:
    def __init__(self, schema='source'):
        load_dotenv(find_dotenv())

        
        self.base_url = json.loads(os.environ['APIS'])[0]
        self.bearer_token = os.environ['BEARER_TOKEN']
        self.schema = schema
        self.engine = create_engine_postgres()  
        self.metadata = MetaData()
        self.headers = {"Authorization": f"Bearer {self.bearer_token}"}


    def get_last_datetime(self, table_name, fallback_data):
        with self.engine.connect() as conn:
            try:
                log_table = Table("last_update_log", MetaData(), autoload_with=self.engine)

                stmt = (
                    select(log_table.c.report_time)
                    .where(log_table.c.table_name == table_name)
                    .limit(1)
                )

                result = conn.execute(stmt).fetchone()
                if result and result[0]:
                    return result[0]
                else:
                    return datetime.strptime(fallback_data, "%d/%m/%Y %H:%M:%S")

            except SQLAlchemyError as e:
                logger.error(f"Erro ao obter última data de {table_name}: {e}")
                return datetime.strptime(fallback_data, "%d/%m/%Y %H:%M:%S")

    def get_new_url(self, route: str, params: dict) -> str:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.base_url}{route}?{query_string}"
        
    def insert_data(self, table_name, data):
        normalized_data = []
        
        # Retrieve the table and its columns
        table = Table(table_name, self.metadata, autoload_with=self.engine, schema=self.schema)
        columns = [column.name for column in table.columns]
        for row in data:
            new_row = {}

            row = {k.lower(): v for k, v in row.items()}
            for col in columns:
                v = row.get(col, None)
                if isinstance(v, (dict, list)):
                    new_row[col] = json.dumps(v, ensure_ascii=False)
                else:
                    new_row[col] = v
            normalized_data.append(new_row)


        logger.info(f"Dados a inserir não normalizados: {data[0]}")

        logger.info(f"Dados a inserir normalizados: {normalized_data[0]}")
        
        stmt = pg_insert(table).values(normalized_data)
        stmt = stmt.on_conflict_do_nothing()


        # Execute the insert statement
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
        
        logger.info(f"{result.rowcount} de {len(normalized_data)} registros inseridos em {table_name} (conflitos ignorados)")


    def safe_get_request(self, url, headers=None, **kwargs):
        try:
            response = requests.get(url, headers=headers, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "desconhecido"
            response_text = e.response.text.strip() if e.response and e.response.text else "sem conteúdo"
            logger.error(f"[{status_code}] Erro HTTP ao acessar {url} - Resposta: {response_text}")
            raise RuntimeError(f"Falha na requisição HTTP para {url} - status {status_code}") from e
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de rede ao acessar {url}: {str(e)}")
            raise RuntimeError(f"Falha de rede ao acessar {url}") from e


    def fetch_and_insert_data(self, route, table_name, params=None):
        params = params or {}

        # Converte 'auto_update' para booleano se estiver em params
        raw_auto_update = params.pop("auto_update", False)
        auto_update = str(raw_auto_update).lower() == "true"

        # fallback_data só é obrigatório se auto_update for False
        fallback_data = params.get("data")
        if not fallback_data and not auto_update:
            raise ValueError("Parâmetro 'data' obrigatório por enquanto, exceto quando 'auto_update' é True.")

        if auto_update:
            # Usa 00:00 do dia atual
            ultima_data = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # Obtém a última data do log com base no fallback_data
            ultima_data = self.get_last_datetime(table_name, fallback_data)

        logger.info(f"Last Date: {ultima_data}")

        # Atualiza o parâmetro 'data'
        params["data"] = ultima_data.strftime("%d/%m/%Y %H:%M:%S")

        # Monta a URL final
        nova_url = self.get_new_url(route, params)
        logger.info(f"Fetching from URL: {nova_url}")

        response = self.safe_get_request(nova_url, headers=self.headers)
        data = response.json()

        if not isinstance(data, list):
            data = [data]

        if data:
            self.insert_data(table_name, data)

            ultima_data_json = data[-1].get('dataHora')
            if ultima_data_json:
                dt = datetime.strptime(ultima_data_json, "%Y-%m-%d %H:%M:%S")
                update_last_datetime(engine=self.engine, table_name=table_name, report_time=dt)