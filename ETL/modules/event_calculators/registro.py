import pandas as pd
import logging

class RegistroCalculator():
    def __init__(self, base):
        self.base = base

    def calculate(self):
        df = self.base.extract_data_orm(
            'boletim',
            ['pacienteid', 'dataentrada', 'tipoatendimento'],
            'dataentrada'
        )

        df = (
            df
            .drop_duplicates()
            .rename(columns={'dataentrada': 'real_date'})
            .assign(
                event_name='registro',
                spare1=lambda d: d['tipoatendimento'].str.strip(),  # preserva o tipo
                spare2=None,
                real_date=lambda d: pd.to_datetime(d['real_date']),
                trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
            )
        )

        return df[['pacienteid', 'event_name', 'real_date', 'spare1', 'spare2', 'trunc_date']]
