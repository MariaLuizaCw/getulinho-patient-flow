
import pandas as pd
import logging


class ExameCalculator():
    def __init__(self, base):
        self.base = base
    def calculate(self):
        df = self.base.extract_data_orm(
            table_name='exames',
            cols=['pacienteid', 'datarealizacao', 'status', 'exame', 'tipo'],
            time_column='datarealizacao'
        )

        df = (
            df[df['status'] == 'REALIZADO']
            .drop(columns='status')
            .drop_duplicates()
            .rename(columns={
                'datarealizacao': 'real_date',
                'exame': 'spare1',
                'tipo': 'spare2'
            })
            .assign(
                event_name='exame',
                real_date=lambda d: pd.to_datetime(d['real_date']),
                trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
            )
        )

        df = df[['pacienteid', 'event_name', 'real_date', 'spare1', 'spare2', 'trunc_date']]
        return df
