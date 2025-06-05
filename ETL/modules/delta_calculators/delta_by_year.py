# /delta_calculators/delta_by_year.py

from datetime import datetime

class DeltaByYear:
    def __init__(self, delta_base):
        """
        A classe DeltaByYear recebe uma instância da classe DeltaBase.
        :param delta_base: Instância da classe DeltaBase
        """
        self.fetcher = delta_base  # Passando DeltaBase como dependência

    def calculate(self, year, sequence=None):
        """
        Calcula os deltas para um ano específico.
        :param year: Ano para calcular os deltas.
        :param sequence: Sequência de eventos a ser usada para o cálculo.
        :return: Resultados do cálculo dos deltas.
        """
        start_interval = datetime(year, 1, 1)  # Início do ano
        end_interval = datetime(year + 1, 1, 1)  # Fim do ano (primeiro dia do próximo ano)

        # Obter valores distintos de spare1 e spare2
        spare1_values, spare2_values = self.fetcher.get_distinct_spare_values()

        # Executar a consulta SQL e obter os resultados
        results = self.fetcher.execute_delta_query(start_interval, end_interval, sequence, spare1_values, spare2_values)

        # Processar os resultados (aqui você pode imprimir ou fazer outro tipo de processamento)
        for row in results:
            print(row)

        return results, start_interval
