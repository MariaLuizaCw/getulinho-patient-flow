import pandas as pd

class Internacao1Calculator():
    def __init__(self, base):
        self.base = base

    def calculate(self):
        df_internacao = self.base.extract_data_orm(
            'boletim',
            ['pacienteid', 'datainternacao', 'tipoatendimento'],
            'datainternacao'
        ).drop_duplicates()

        df_internacao['tipoatendimento'] = df_internacao['tipoatendimento'].str.strip()

        df_consulta = self.base.extract_data_orm(
            'atendimentopacientes',
            ['pacienteid', 'tipoatendimento', 'dthrfim'],
            'dthrfim',
            last_x_hours=24
        ).sort_values('dthrfim', ascending=False)

        df_consulta['tipoatendimento'] = df_consulta['tipoatendimento'].str.strip()

        df_merged = pd.merge(df_internacao, df_consulta, on='pacienteid', how='inner')
        df_merged['dthrfim'] = pd.to_datetime(df_merged['dthrfim'])
        df_merged = df_merged.sort_values(['pacienteid', 'dthrfim'], ascending=[True, False])
        df_last = df_merged.drop_duplicates('pacienteid', keep='first')
        df_last_atendimento = df_last[df_last['tipoatendimento_y'] == 'ATENDIMENTO']

        df_last_atendimento = df_last_atendimento.rename(columns={'datainternacao': 'real_date'})
        df_last_atendimento = df_last_atendimento.assign(
            event_name='inicio_internacao1',
            spare1=lambda d: d['tipoatendimento_x'].str.strip(),
            spare2=None,
            real_date=lambda d: pd.to_datetime(d['real_date']),
            trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
        )

        return df_last_atendimento[['pacienteid', 'event_name', 'real_date', 'spare1', 'spare2', 'trunc_date']]

class Internacao2Calculator():
    def __init__(self, base):
        self.base = base

    def calculate(self):
        df_internacao = self.base.extract_data_orm(
            'boletim',
            ['pacienteid', 'datainternacao', 'tipoatendimento'],
            'datainternacao'
        ).drop_duplicates()

        df_internacao['tipoatendimento'] = df_internacao['tipoatendimento'].str.strip()

        df_consulta = self.base.extract_data_orm(
            'atendimentopacientes',
            ['pacienteid', 'tipoatendimento', 'dthrfim'],
            'dthrfim',
            last_x_hours=24
        ).sort_values('dthrfim', ascending=False)

        df_consulta['tipoatendimento'] = df_consulta['tipoatendimento'].str.strip()

        df_merged = pd.merge(df_internacao, df_consulta, on='pacienteid', how='inner')
        df_merged['dthrfim'] = pd.to_datetime(df_merged['dthrfim'])
        df_merged = df_merged.sort_values(['pacienteid', 'dthrfim'], ascending=[True, False])
        df_last = df_merged.drop_duplicates('pacienteid', keep='first')
        df_last_reavaliacao = df_last[df_last['tipoatendimento_y'] == 'REAVALIACAO']

        df_last_reavaliacao = df_last_reavaliacao.rename(columns={'datainternacao': 'real_date'})
        df_last_reavaliacao = df_last_reavaliacao.assign(
            event_name='inicio_internacao2',
            spare1=lambda d: d['tipoatendimento_x'].str.strip(),
            spare2=None,
            real_date=lambda d: pd.to_datetime(d['real_date']),
            trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
        )

        return df_last_reavaliacao[['pacienteid', 'event_name', 'real_date', 'spare1', 'spare2', 'trunc_date']]