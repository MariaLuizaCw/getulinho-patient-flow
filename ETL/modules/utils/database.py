
from sqlalchemy import create_engine
import os
import logging
from sqlalchemy import Table, MetaData, insert
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert as pg_insert 
from utils.help import get_now_brazil_full
import logging


logger = logging.getLogger(__name__)

def create_engine_postgres():
    db_url = os.environ.get("AIRFLOW_CONN_POSTGRES_MASTER")
    try:
        engine = create_engine(db_url)
        logger.info("Engine SQLAlchemy criada com sucesso.")
        return engine
    except Exception as e:
        logger.exception("Erro ao criar engine SQLAlchemy.")
        raise

def update_last_datetime(engine, table_name, report_time):
    report_time_naive = report_time.replace(tzinfo=None)
    update_time_naive = get_now_brazil_full().replace(tzinfo=None)

    with engine.begin() as conn:
        try:
            metadata = MetaData()
            log_table = Table("last_update_log", metadata, autoload_with=engine)

            stmt = pg_insert(log_table).values(
                table_name=table_name,
                report_time=report_time_naive,
                update_time=update_time_naive
            ).on_conflict_do_update(
                index_elements=['table_name'],
                set_={
                    'report_time': report_time_naive,
                    'update_time': update_time_naive
                }
            )

            conn.execute(stmt)

        except SQLAlchemyError as e:
            logger.error(f"Erro ao atualizar last_update_log: {e}")