
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import MetaData, Table, select, and_
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv, find_dotenv
from utils.database import create_engine_postgres  


load_dotenv(find_dotenv())
logger = logging.getLogger(__name__)

class BaseEventCalculator():
    def __init__(self, source_schema, current_hour, last_x_hours=1):
        self.source_schema = source_schema
        self.engine = create_engine_postgres() 
        self.metadata = MetaData()
        self.current_hour = current_hour
        self.last_x_hours = last_x_hours


    def interval(self, timestamp, interv=1):
        return timestamp - timedelta(hours=interv)

    def extract_data_orm(self, table_name: str, cols: list, time_column: str, last_x_hours=None):
        if last_x_hours is None:
            last_x_hours = self.last_x_hours

        past_hour = self.interval(self.current_hour, last_x_hours)
        logger.info(f"[extract_data_orm] Using past_hour: {past_hour} and current_hour: {self.current_hour}")
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine, schema=self.source_schema)
            

            Session = sessionmaker(bind=self.engine)
            with Session() as session:
                stmt = (
                    select(*[table.c[c] for c in cols])
                    .where(table.c[time_column].between(past_hour.replace(tzinfo=None), self.current_hour.replace(tzinfo=None)))
                    .order_by(table.c[time_column].asc())
                )
                result = session.execute(stmt).fetchall()
                df = pd.DataFrame(result, columns=cols)
                logger.info(f"[extract_data_orm] {len(df)} rows from {table_name}")
                return df
        except Exception as e:
            logger.error(f"[extract_data_orm] Failed to query {table_name}: {e}")
            return pd.DataFrame(columns=cols)


