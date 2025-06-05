from datetime import datetime, timedelta

class DeltaByMonth:
    def __init__(self, delta_base):
        self.fetcher = delta_base  # Passando DeltaBase como dependência

    def calculate(self, sequence=None):
    
        current_month = self.fetcher.current_hour.month
        current_year = self.fetcher.current_hour.year
        current_day = self.fetcher.current_hour.day

   
        if current_day <= 7:  
            if current_month == 1:
                start_interval = datetime(current_year - 1, 12, 1)  # Janeiro, então pega o mês de dezembro do ano anterior
                end_interval = datetime(current_year, 1, 1) - timedelta(seconds=1)  # Último dia de dezembro
            else:
                start_interval = datetime(current_year, current_month - 1, 1)  # Primeiro dia do mês anterior
                end_interval = datetime(current_year, current_month, 1) - timedelta(seconds=1)  # Último dia do mês anterior
        else:  
            start_interval = datetime(current_year, current_month, 1)  # Primeiro dia do mês atual
            end_interval = datetime(current_year, current_month + 1, 1) - timedelta(seconds=1)  # Último dia do mês atual

        spare_columns = self.fetcher.get_distinct_spare_values(sequence['start_event'], sequence['end_event'], start_interval, end_interval)
        results = self.fetcher.execute_delta_query(start_interval, end_interval, sequence, spare_columns)

        return results, start_interval
