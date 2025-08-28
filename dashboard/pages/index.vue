<template>
  <v-container>
    <v-container>
      <div class="text-h4 font-weight-black">
        Lasos FluxoSaúde: Painel de Monitoramento Inteligente do Fluxo Hospitalar
      </div>

    </v-container>
    <!-- Título / Intervalo de dados -->
    <v-container class="mt-2">

 
        <v-row>
          <v-col cols="6">
            <RiskFilter 
              :default-risks="filters.risks" 
              @filters-changed="updateFilters" 
            />
          </v-col>
          <v-col cols="6">
            <TimeFilter 
              :default-time-range="filters.time_range" 
              @filters-changed="updateFilters" 
            />
          </v-col>
      </v-row>

 
    </v-container>

    <!-- Componente de visualização do fluxo -->
    <v-container class="mt-2">
      <Flow :deltas="flowData"   @node-clicked="handleNodeClick" @edge-clicked="handleEdgeClick"  />
      <div class="text-subtitle-1" v-if="flowData?.length">
        Intervalo de dados do Fluxo: {{ formatDateTime(flowData[0].start_interval) }} - {{ formatDateTime(flowData[0].end_interval) }}
      </div>
    </v-container>

  <v-container v-if="selectedNode">
    <v-row> 
      <v-col cols="2">Teste</v-col>
      <v-col cols="5">Teste</v-col>
<v-col cols="5">
  <Lineplot :filters="filters" :selectedNode="selectedNode"/>
</v-col>
    </v-row>
     
  </v-container>

  </v-container>
</template>

<script>
import RiskFilter from '~/components/filters/RiskFilter.vue';
import TimeFilter from '~/components/filters/TimeFilter.vue';
import Flow from '~/components/Flow.vue';
import { postgrestFetch } from '~/utils/postgrestFetch.js';
import { timeFilters, registerTimeFilters } from '~/components/filters/filters.js';
import Lineplot from '~/components/Lineplot.vue';

export default {
  components: { RiskFilter, TimeFilter, Flow },
  data() {
    return {
      filters: {
        risks: ["Azul", "Verde", "Amarelo", "Vermelho", "Indefinido"],
        time_range: '24 horas',
        time_table: 'events_last_24h',
        time_complement: {} // vazio por padrão
      },
      flowData: null,
      selectedNode: null
    };
  },
  created() {
    registerTimeFilters(); // registra filtros de tempo
    this.fetchFlowData();  // busca dados iniciais
  },
  methods: {
    // Formata datas YYYY-MM-DD sem timezone
    formatDate(value) {
      if (!value) return "";
      if (typeof value === "string") return value.substr(0, 10);
      return new Date(value).toISOString().substr(0, 10);
    },

    formatDateTime(value) {
      if (!value) return "";
      const date = typeof value === "string" ? new Date(value) : value;

      const pad = (n) => String(n).padStart(2, "0");

      const year = date.getFullYear();
      const month = pad(date.getMonth() + 1);
      const day = pad(date.getDate());
      const hours = pad(date.getHours());
      const minutes = pad(date.getMinutes());
      const seconds = pad(date.getSeconds());

      return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    },

    // Faz fetch dos dados aplicando filtros
    async fetchFlowData() {
      const queryParams = new URLSearchParams();

      // Filtro de risco
      if (this.filters.risks?.length) {
        const risksUpper = this.filters.risks.map(r => r.toUpperCase());
        queryParams.append('risk_class', `in.(${risksUpper.join(',')})`);
      }

      // Filtro de tempo usando timeFilters
      const timeFilterFn = timeFilters[this.filters.time_range];
      if (timeFilterFn) {
        await timeFilterFn(queryParams, this.filters.time_table, this.filters.time_complement);
      }

      const url = `/${this.filters.time_table}?${queryParams.toString()}`;
      try {
        const response = await postgrestFetch(url);
        this.flowData = response['data']

      } catch (error) {
        console.error('Erro ao buscar dados do PostgREST:', error);
      }
    },

    // Atualiza filtros emitidos pelos filhos e dispara fetch
    async updateFilters(newFilters) {
      this.filters = { ...this.filters, ...newFilters };
      await this.fetchFlowData();
    },

    handleNodeClick(node) {
      console.log("Recebido do filho, nó clicado:", node);
    },
    handleEdgeClick(edge) {
      // Parse se for string
      const edgeObj = typeof edge === 'string' ? JSON.parse(edge) : edge;

      const deltaName = edgeObj.delta_name;
      const deltaDisplayName = `Tempo entre ${edgeObj.display_name_start.replace(/\n/g, '')} e ${edgeObj.display_name_end.replace(/\n/g, '')}`;

      this.selectedNode = {
        delta_name: deltaName,
        delta_display_name: deltaDisplayName
      };
    }
  }
};
</script>

<style scoped>
/* Adicione estilos se necessário */
</style>
