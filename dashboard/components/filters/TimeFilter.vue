<template>
  <div>
    <v-select
      v-model="selectedTimeRange"
      :items="Object.keys(timeOptions)"
      label="Tamanho da Janela"
      color="primary"
      @update:modelValue="emitSelection"
    />

    <div v-if="selectedTimeRange === 'Dia'" class="mt-2">
      <v-date-input
        v-model="day"
        label="Data selecionada:"
        :max="yesterday"
        color="primary"
      />
    </div>

    <div v-if="selectedTimeRange === 'Mês'" class="mt-2">
      <v-row>
        <v-col cols="6">
          <v-text-field
            v-model="monthYear"
            label="Ano"
            type="number"
            :min="2021"
            :max="new Date().getFullYear()"
          />
        </v-col>
        <v-col cols="6">
          <v-select
            v-model="month"
            :items="months"
            label="Mês"
          />
        </v-col>
      </v-row>
    </div>

    <div v-if="selectedTimeRange === 'Ano'" class="mt-2">
      <v-text-field
        v-model="year"
        label="Ano"
        type="number"
        :min="2021"
        :max="new Date().getFullYear()"
      />
    </div>
  </div>
</template>

<script>
export default {
  name: 'TimeFilter',
  props: {
    defaultTimeRange: {
      type: String,
      default: '24 horas'
    }
  },
  data() {
    const today = new Date();
    const months = [
      'Janeiro','Fevereiro','Março','Abril','Maio','Junho',
      'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'
    ];

    let monthIndex;
    if (today.getDate() > 15) {
      monthIndex = today.getMonth(); // mês atual
    } else {
      monthIndex = (today.getMonth() + 11) % 12; // mês anterior
    }

    return {
      selectedTimeRange: this.defaultTimeRange,
      yesterday:  new Date(Date.now() - 86400000).toISOString().slice(0, 10),
      day: new Date(Date.now() - 86400000).toISOString().slice(0, 10),
      monthYear: new Date().getFullYear(),
      month: months[monthIndex],
      year: new Date().getFullYear(),
      months: [
        'Janeiro','Fevereiro','Março','Abril','Maio','Junho',
        'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'
      ],
      timeOptions: {
        '4 horas': 'events_last_4h',
        '24 horas': 'events_last_24h',
        'Dia': 'events_day',
        'Mês': 'events_month',
        'Ano': 'events_year'
      }
    }
  },
  methods: {
    formatDate(date) {
      if (!date) return "";
      if (typeof date === "string") return date; // já está no formato correto
      return date.toISOString().slice(0, 10);   // só converte se for Date
    },
    emitSelection() {
      const payload = {
        time_range: this.selectedTimeRange,
        time_table: this.timeOptions[this.selectedTimeRange],
        time_complement: {
          ...(this.selectedTimeRange === 'Dia' && { day: this.formatDate(this.day) }),
          ...(this.selectedTimeRange === 'Mês' && { year: this.monthYear, month: this.month }),
          ...(this.selectedTimeRange === 'Ano' && { year: this.year })
        }
      };
      this.$emit('filters-changed', payload);
    }
  },
  watch: {
    day: 'emitSelection',
    monthYear: 'emitSelection',
    month: 'emitSelection',
    year: 'emitSelection'
  },
  mounted() {
    this.emitSelection(); // envia valor inicial para o pai
  }
}
</script>
