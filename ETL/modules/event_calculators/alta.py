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





class Alta1Calculator():
    def __init__(self, base):
        self.base = base

    def calculate(self):
        # Extrai dados de alta do boletim
        df_alta = self.base.extract_data_orm(
            'boletim',
            ['pacienteid', 'dataalta', 'tipoatendimento'],
            'dataalta'
        ).drop_duplicates()

        df_alta['tipoatendimento'] = df_alta['tipoatendimento'].str.strip()

        # Extrai últimas consultas em até 24h
        df_consulta = self.base.extract_data_orm(
            'atendimentopacientes',
            ['pacienteid', 'tipoatendimento', 'dthrfim'],
            'dthrfim',
            last_x_hours=24
        ).sort_values('dthrfim', ascending=False)

        df_consulta['tipoatendimento'] = df_consulta['tipoatendimento'].str.strip()

        # Junta dados de alta com consultas
        df_merged = pd.merge(df_alta, df_consulta, on='pacienteid', how='inner')
        df_merged['dthrfim'] = pd.to_datetime(df_merged['dthrfim'])
        df_merged = df_merged.sort_values(['pacienteid', 'dthrfim'], ascending=[True, False])
        df_last = df_merged.drop_duplicates('pacienteid', keep='first')
        df_last_atendimento = df_last[df_last['tipoatendimento_y'] == 'ATENDIMENTO']

        # Prepara saída
        df_last_alta = df_last_atendimento.rename(columns={'dataalta': 'real_date'})
        df_last_alta['real_date'] = pd.to_datetime(df_last_alta['real_date'])

        # Ajuste: se dataalta <= dthrfim, corrige para dthrfim + 1min
        df_last_alta.loc[df_last_alta['real_date'] <= df_last_alta['dthrfim'], 'real_date'] = \
            df_last_alta['dthrfim'] + pd.Timedelta(minutes=1)

        df_last_alta = df_last_alta.assign(
            event_name='alta1',
            spare1=lambda d: d['tipoatendimento_x'].str.strip(),
            spare2=None,
            trunc_date=lambda d: d['real_date'].dt.floor('h')
        )

        return df_last_alta[['pacienteid', 'event_name', 'real_date', 'spare1', 'spare2', 'trunc_date']]


class Alta2Calculator():
    def __init__(self, base):
        self.base = base

    def calculate(self):
        # Extrai dados de alta do boletim
        df_alta = self.base.extract_data_orm(
            'boletim',
            ['pacienteid', 'dataalta', 'tipoatendimento'],
            'dataalta'
        ).drop_duplicates()

        df_alta['tipoatendimento'] = df_alta['tipoatendimento'].str.strip()

        # Extrai últimas consultas em até 24h
        df_consulta = self.base.extract_data_orm(
            'atendimentopacientes',
            ['pacienteid', 'tipoatendimento', 'dthrfim'],
            'dthrfim',
            last_x_hours=24
        ).sort_values('dthrfim', ascending=False)

        df_consulta['tipoatendimento'] = df_consulta['tipoatendimento'].str.strip()

        # Junta dados de alta com consultas
        df_merged = pd.merge(df_alta, df_consulta, on='pacienteid', how='inner')
        df_merged['dthrfim'] = pd.to_datetime(df_merged['dthrfim'])
        df_merged = df_merged.sort_values(['pacienteid', 'dthrfim'], ascending=[True, False])
        df_last = df_merged.drop_duplicates('pacienteid', keep='first')
        df_last_reavaliacao = df_last[df_last['tipoatendimento_y'] == 'REAVALIACAO']

        # Prepara saída
        df_last_alta = df_last_reavaliacao.rename(columns={'dataalta': 'real_date'})
        df_last_alta['real_date'] = pd.to_datetime(df_last_alta['real_date'])

        # Ajuste: se dataalta <= dthrfim, corrige para dthrfim + 1min
        df_last_alta.loc[df_last_alta['real_date'] <= df_last_alta['dthrfim'], 'real_date'] = \
            df_last_alta['dthrfim'] + pd.Timedelta(minutes=1)

        df_last_alta = df_last_alta.assign(
            event_name='alta2',
            spare1=lambda d: d['tipoatendimento_x'].str.strip(),
            spare2=None,
            trunc_date=lambda d: d['real_date'].dt.floor('h')
        )

        return df_last_alta[['pacienteid', 'event_name', 'real_date', 'spare1', 'spare2', 'trunc_date']]