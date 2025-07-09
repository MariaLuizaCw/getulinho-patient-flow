<template>
  <v-container>
    <v-container>
      <div class="text-h4 font-weight-black">
        Lasos FluxoSaúde: Painel de Monitoramento Inteligente do Fluxo Hospitalar
      </div>

      <div class="text-subtitle-1">
        Receba o emitido do painel de filtros e envie para o fluxo
      </div>

      <FiltersPanel @filters-changed="updateFilters" />  <!-- Ouve o evento 'filters-changed' -->
    </v-container>

    <v-container>
      <Flow :filters="filters" />  <!-- Passa os filtros para o Flow -->
    </v-container>
  </v-container>
</template>

<script>
import FiltersPanel from '~/components/FiltersPanel.vue'
import Flow from '~/components/Flow.vue'


export default {
  components: {
    FiltersPanel,
    Flow
  },
  data() {
    return {
      filters: {}  // Filtros que serão passados para o Flow
    }
  },
  methods: {
    updateFilters(filters) {
      this.filters = filters
      console.log('Updated filters:', filters)
    }
  }
}
</script>
