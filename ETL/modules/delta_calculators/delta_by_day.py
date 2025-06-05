# /delta_calculators/delta_by_day.py
from datetime import datetime, timedelta


class DeltaByDay:
    def __init__(self, delta_base):
        self.fetcher = delta_base 

    def calculate(self, sequence=None):
        end_interval = datetime(self.fetcher.current_hour.year, self.fetcher.current_hour.month, self.fetcher.current_hour.day)
        start_interval = end_interval - timedelta(days=1)
        spare_columns = self.fetcher.get_distinct_spare_values(sequence['start_event'], sequence['end_event'], start_interval, end_interval)
        results = self.fetcher.execute_delta_query(start_interval, end_interval, sequence, spare_columns)


        return results, start_interval
