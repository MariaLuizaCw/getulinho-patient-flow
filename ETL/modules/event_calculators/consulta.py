
import pandas as pd

class InicioConsulta1Calculator():
    def __init__(self, base):
        self.base = base
    def calculate(self):
        df = self.base.extract_data_orm('atendimentopacientes', ['pacienteid', 'dthrinicio', 'tipoatendimento'], 'dthrinicio')
        df = df[df['tipoatendimento'] == 'ATENDIMENTO']
        df = df.drop(columns='tipoatendimento').drop_duplicates().rename(columns={'dthrinicio': 'real_date'})

        return df.assign(
            event_name='inicio_consulta1',
            spare1=None,
            spare2=None,
            trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
        )[["pacienteid", "event_name", "real_date", "spare1", "spare2", "trunc_date"]]

class FimConsulta1Calculator():
    def __init__(self, base):
        self.base = base
    def calculate(self):
        df = self.base.extract_data_orm('atendimentopacientes', ['pacienteid', 'dthrfim', 'tipoatendimento'], 'dthrfim')
        df = df[df['tipoatendimento'] == 'ATENDIMENTO']
        df = df.drop('tipoatendimento', axis=1).drop_duplicates().rename(columns={'dthrfim': 'real_date'})

        return df.assign(
            event_name='fim_consulta1',
            spare1=None,
            spare2=None,
            trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
        )[["pacienteid", "event_name", "real_date", "spare1", "spare2", "trunc_date"]]


class InicioConsulta2Calculator():
    def __init__(self, base):
        self.base = base
    def calculate(self):
        df = self.base.extract_data_orm('atendimentopacientes', ['pacienteid', 'dthrinicio', 'tipoatendimento'], 'dthrinicio')
        df = df[df['tipoatendimento'] == 'REAVALIACAO']
        df = df.drop('tipoatendimento', axis=1).drop_duplicates().rename(columns={'dthrinicio': 'real_date'})

        return df.assign(
            event_name='inicio_consulta2',
            spare1=None,
            spare2=None,
            trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
        )[["pacienteid", "event_name", "real_date", "spare1", "spare2", "trunc_date"]]


class FimConsulta2Calculator():
    def __init__(self, base):
        self.base = base
    def calculate(self):
        df = self.base.extract_data_orm('atendimentopacientes', ['pacienteid', 'dthrfim', 'tipoatendimento'], 'dthrfim')
        df = df[df['tipoatendimento'] == 'REAVALIACAO']
        df = df.drop('tipoatendimento', axis=1).drop_duplicates().rename(columns={'dthrfim': 'real_date'})

        return df.assign(
            event_name='fim_consulta2',
            spare1=None,
            spare2=None,
            trunc_date=lambda d: pd.to_datetime(d['real_date']).dt.floor('h')
        )[["pacienteid", "event_name", "real_date", "spare1", "spare2", "trunc_date"]]