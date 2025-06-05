import pandas as pd

class AltaCalculator:
    def __init__(self, base):
        self.base = base

    def calculate(self):
        # Extrai dados da tabela 'episodio'
        df = self.base.extract_data_orm(
            table_name='episodio',
            cols=['pacienteid', 'dataalta', 'motivoalta', 'tipoatendimento'],
            time_column='dataalta'
        )

        df = (
            df
            .drop_duplicates()
            .rename(columns={'dataalta': 'real_date'})
            .assign(
                event_name='registro',
                spare1=lambda d: d['tipoatendimento'].str.strip(),
                spare2=lambda d: d['motivoalta'].str.strip(),
                real_date=lambda d: pd.to_datetime(d['real_date']),
                trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
            )
        )

        return df[['pacienteid', 'event_name', 'real_date', 'spare1', 'spare2', 'trunc_date']]



