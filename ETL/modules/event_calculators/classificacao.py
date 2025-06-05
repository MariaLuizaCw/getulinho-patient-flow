
import pandas as pd

class InicioClassificacaoCalculator():
    def __init__(self, base):
        self.base = base
    def calculate(self):
        df = self.base.extract_data_orm('classificacaorisco', ['pacienteid', 'datainicio'], 'datainicio')
        return df.drop_duplicates().rename(columns={'datainicio': 'real_date'}).assign(
            event_name='inicio_classificacao',
            spare1=None,
            spare2=None,
            trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
        )[["pacienteid", "event_name", "real_date", "spare1", "spare2", "trunc_date"]]


class FimClassificacaoCalculator():
    def __init__(self, base):
        self.base = base
    def calculate(self):
        df = self.base.extract_data_orm('classificacaorisco', ['pacienteid', 'datafim'], 'datafim')
        return df.drop_duplicates().rename(columns={'datafim': 'real_date'}).assign(
            event_name='fim_classificacao',
            spare1=None,
            spare2=None,
            trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
        )[["pacienteid", "event_name", "real_date", "spare1", "spare2", "trunc_date"]]
    def get_class(self):
        df = self.base.extract_data_orm(
            table_name='classificacaorisco',
            cols=['pacienteid', 'datafim', 'risco'],
            time_column='datafim'
        )
        
        df = df.drop_duplicates(subset=['pacienteid', 'datafim', 'risco'])

        df = df.rename(columns={
            'datafim': 'date',
            'risco': 'risk_class'
        })

        return df[['pacienteid', 'date', 'risk_class']]