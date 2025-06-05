<template>
  <div>
    <v-select
      v-model="selectedTimeRange"
      :items="Object.keys(timeOptions)"
      label="Intervalo de Tempo"
      color="primary"
      @update:modelValue="emitSelection"
    />

    <div v-if="selectedTimeRange === 'Selecionar um dia'" class="mt-2">
      <v-date-input
      v-model="day"
      label="Data selecionada:"
      :max="today"
      color="primary"
      prepend-icon=""
      append-inner-icon="mdi-calendar"
    />
    </div>

    <div v-if="selectedTimeRange === 'Selecionar um mês'" class="mt-2">
      <v-row>
        <v-col cols="6">
          <v-text-field
            v-model="monthYear"
            label="Ano"
            type="number"
            :min="2021"
            :max="new Date().getFullYear()"
            @input="emitSelection"
          />
        </v-col>
        <v-col cols="6">
          <v-select
            v-model="month"
            :items="months"
            label="Mês"
            @update:modelValue="emitSelection"
          />
        </v-col>
      </v-row>
    </div>

    <div v-if="selectedTimeRange === 'Selecionar um ano'" class="mt-2">
      <v-text-field
        v-model="year"
        label="Ano"
        type="number"
        :min="2021"
        :max="new Date().getFullYear()"
        @input="emitSelection"
      />
    </div>
  </div>
</template>

<script>
export default {
  name: 'TimeFilter',

  data() {
    return {
      selectedTimeRange: 'Últimas 24 horas',
      today: new Date().toISOString().substr(0, 10),
      day: new Date().toISOString().substr(0, 10),
      monthYear: new Date().getFullYear(),
      month: 'Junho',
      year: new Date().getFullYear(),
      months: [
        'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
        'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
      ],
      timeOptions: {
        'Últimas 4 horas': 'events_last_4h',
        'Últimas 24 horas': 'events_last_24h',
        'Selecionar um dia': 'events_day',
        'Selecionar um mês': 'events_month',
        'Selecionar um ano': 'events_year'
      }
    }
  },

  methods: {
    emitSelection() {
      this.$emit('change', {
        time_range: this.selectedTimeRange,
        time_table: this.timeOptions[this.selectedTimeRange],
        time_complement: {
          ...(this.selectedTimeRange === 'Selecionar um dia' && { day: this.day }),
          ...(this.selectedTimeRange === 'Selecionar um mês' && { year: this.monthYear, month: this.month }),
          ...(this.selectedTimeRange === 'Selecionar um ano' && { year: this.year })
        }
      })
    }
  }
}
</script>
