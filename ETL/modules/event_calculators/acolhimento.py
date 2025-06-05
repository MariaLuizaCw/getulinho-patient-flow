
import pandas as pd

class AcolhimentoCalculator():    
    def __init__(self, base):
        self.base = base

    def calculate(self):
        df = self.base.extract_data_orm('acolhimento', ['nome', 'data'], 'data')
        df = df.sort_values('data', ascending=False).drop_duplicates('nome')
        nomes = df['nome'].tolist()

        # Reaproveita a mesma conexão
        pacientes_table = self.base.extract_data_orm('pacientes', ['id', 'nome', 'datahora'], 'datahora', 24)
        df_pacientes = pacientes_table[pacientes_table['nome'].isin(nomes)]
        df_pacientes = df_pacientes.sort_values('datahora', ascending=False).drop_duplicates('nome')

        df = (
            df.merge(df_pacientes, on='nome', how='inner')
            .rename(columns={'data': 'real_date', 'id': 'pacienteid'})
            .loc[:, ['pacienteid', 'real_date']]
            .assign(
                event_name='acolhimento',
                spare1=None,
                spare2=None,
                real_date=lambda d: pd.to_datetime(d['real_date']),
                trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
            )
        )
        return df[['pacienteid', 'event_name', 'real_date', 'spare1', 'spare2', 'trunc_date']]
