# /delta_calculators/delta_by_hours.py

from datetime import timedelta

class DeltaByHour:
    def __init__(self, delta_base):
        self.fetcher = delta_base  # Passando DeltaBase como dependência

    def calculate(self,  sequence=None, last_x_hours=None):
        start_interval = self.fetcher.current_hour - timedelta(hours=last_x_hours)
        end_interval = self.fetcher.current_hour

        # Obter valores distintos de spare1 e spare2
        spare_columns = self.fetcher.get_distinct_spare_values(sequence['start_event'], sequence['end_event'], start_interval, end_interval)

        # Executar a consulta SQL e obter os resultados
        results = self.fetcher.execute_delta_query(start_interval, end_interval, sequence, spare_columns)

        return results, start_interval
