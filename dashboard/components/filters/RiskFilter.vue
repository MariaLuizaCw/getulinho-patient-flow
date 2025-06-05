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

  data() {
    return {
      riskClasses: ["Azul", "Verde", "Amarelo", "Vermelho", "Indefinido"],
      selectedRisks: ["Azul", "Verde", "Amarelo", "Vermelho", "Indefinido"]
    }
  },

  methods: {
    emitSelection() {
      this.$emit('change', this.selectedRisks)
    }
  }
}
</script>
