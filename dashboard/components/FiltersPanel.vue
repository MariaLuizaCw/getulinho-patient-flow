<template>
    <v-row>
      <v-col cols="6">
        <RiskFilter @change="handleRiskChange" />
      </v-col>
      <v-col cols="6">
        <TimeFilter @change="handleTimeChange" ref="timeFilter" />
      </v-col>
    </v-row>
</template>

<script>
import RiskFilter from './filters/RiskFilter.vue'
import TimeFilter from './filters/TimeFilter.vue'

export default {
  name: 'FiltersPanel',
  components: {
    RiskFilter,
    TimeFilter
  },

  data() {
    return {
      selectedRisks: [],
      selectedTime: {}
    }
  },

  methods: {
    handleRiskChange(value) {
      this.selectedRisks = value
      console.log('Risks changed:', value)
    },
    handleTimeChange(value) {
      this.selectedTime = value
      console.log('Time changed:', value)
    },
    emitFilters() {
      const filters = {
        risks: this.selectedRisks,
        ...this.selectedTime
      }

      console.log('Filtros finais:', filters)
      this.$emit('change', filters)
    }
  }
}
</script>
