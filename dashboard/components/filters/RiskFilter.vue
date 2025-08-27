<template>
  <v-select
    v-model="selectedRisks"
    :items="riskClasses"
    label="Classificação de Risco"
    multiple
    chips
    color="primary"
    :chip-props="{ color: 'primary', textColor: 'white' }"
    @update:modelValue="emitSelection"
  />
</template>

<script>
export default {
  name: 'RiskFilter',
  props: {
    defaultRisks: {
      type: Array,
      default: () => ["Azul", "Verde", "Amarelo", "Vermelho", "Indefinido"]
    }
  },
  data() {
    return {
      riskClasses: ["Azul", "Verde", "Amarelo", "Vermelho", "Indefinido"],
      selectedRisks: this.defaultRisks
    }
  },
  methods: {
    emitSelection() {
      this.$emit('filters-changed', { risks: this.selectedRisks });
    }
  },
  mounted() {
    this.emitSelection(); // envia valor inicial para o pai
  }
}
</script>
