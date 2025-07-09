<template>
  <v-container>
    <div class="text-h4 font-weight-black">
      Lasos FluxoSaúde: Painel de Monitoramento Inteligente do Fluxo Hospitalar
    </div>

    <div class="text-subtitle-1">
      Receba o emitido do painel de filtros e envie para o fluxo
    </div>

    <RiskFilter
      @filters-changed="updateFilters"
    />
    
    <TimeFilter
      @filters-changed="updateFilters"
    />
  </v-container>

  <v-container>
    <!-- Passa os filtros atualizados para o Flow -->
    <Flow :filters="filters" />
  </v-container>
</template>

<script>
import RiskFilter from '~/components/filters/RiskFilter.vue';
import TimeFilter from '~/components/filters/TimeFilter.vue';
import Flow from '~/components/Flow.vue';
import { registerTimeFilters } from '~/filters/filters.js';

export default {
  components: {
    RiskFilter,
    TimeFilter,
    Flow
  },
  data() {
    return {
      filters: {}
    };
  },
  methods: {
  
    
    async updateFilters(filters) {
      this.filters = filters;
      console.log('Updated filters:', filters);

      const queryParams = new URLSearchParams();

      // Passa time_table e outros filtros
      this.addRiskFilter(queryParams, filters.risks);
      await this.addTimeFilter(queryParams, filters.time_range, filters.time_complement);

      this.fetchData(queryParams);
    },

    addRiskFilter(queryParams, risks) {
      if (risks && risks.length > 0) {
        queryParams.append('risk_class', `in.(${risks.join(',')})`);
      }
    },

    // Chama a função de filtro baseada no time_table
    async addTimeFilter(queryParams, timeRange, timeComplement) {
      const timeTable = timeFilters[timeRange]; // Aqui pegamos a tabela correta com base no filtro

      if (timeTable) {
        await timeTable(queryParams, timeComplement, timeTable);
      }
    },

    // Função para buscar dados
    async fetchData(queryParams) {
      try {
        const postgrestUrl = useRuntimeConfig().public.postgrestBaseUrl;
        const url = `${postgrestUrl}/events?${queryParams.toString()}`;
        const response = await $fetch(url);
        console.log(url);
        if (response) {
          console.log('Data received:', response);
        } else {
          console.error('Error fetching data');
        }
      } catch (error) {
        console.error('Error making request:', error);
      }
    }
  },
  created() {
    registerTimeFilters();  // Registra os filtros de tempo
  }
};
</script>
