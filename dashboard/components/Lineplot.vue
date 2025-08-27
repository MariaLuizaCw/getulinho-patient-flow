<template>
  <div>
    <label>Selecione o intervalo de datas:</label>
    <v-range-slider
      v-model="range"
      :min="0"
      :max="dateOptions.length - 1"
      :step="1"
      thumb-label
      color="primary"
    >
      <template v-slot:thumb-label="{ modelValue }">
        {{ dateOptions[modelValue] }}
      </template>
    </v-range-slider>

    <apexchart
      type="line"
      height="350"
      :options="chartOptions"
      :series="series"
    ></apexchart>
  </div>
</template>

<script>
import { postgrestFetch } from '~/utils/postgrestFetch'
import { combinedMean } from '~/utils/flow_functions'

export default {
  props: {
    filters: { type: Object, required: true },
    selectedNode: { type: Object, required: true }
  },
  data() {
    return {
      range: [0, 0],
      dateOptions: [],
      chartData: { labels: [], datasets: [{ label: '', data: [] }] }
    };
  },
  mounted() {
    this.generateDateOptions(this.filters.time_range);
    this.range = [0, this.dateOptions.length - 1];
    this.fetchData();
  },
  methods: {
    generateDateOptions(timeRange) {
      const today = new Date();
      this.dateOptions = [];
      const config = {
        '4 horas': { unit: 'day', count: 30 },
        '24 horas': { unit: 'day', count: 30 },
        'Dia': { unit: 'day', count: 60 },
        'Mês': { unit: 'month', count: 12 },
        'Ano': { unit: 'year', count: 5 }
      };
      const setting = config[timeRange];
      if (!setting) return;

      for (let i = setting.count - 1; i >= 0; i--) {
        const d = new Date(today);
        switch (setting.unit) {
          case 'day':
            d.setDate(today.getDate() - i);
            this.dateOptions.push(d.toISOString().substr(0, 10));
            break;
          case 'month':
            d.setMonth(today.getMonth() - i, 1);
            this.dateOptions.push(`${d.getFullYear()}-${(d.getMonth()+1).toString().padStart(2,'0')}`);
            break;
          case 'year':
            d.setFullYear(today.getFullYear() - i, 0, 1);
            this.dateOptions.push(`${d.getFullYear()}`);
            break;
        }
      }
    },

    async fetchData() {
      if (!this.selectedNode || !this.filters) return;

      const startDate = this.dateOptions[this.range[0]] + 'T00:00:00';
      const endDate = this.dateOptions[this.range[1]] + 'T23:59:59';
      const table = this.filters.time_table;
      const delta = this.selectedNode.delta_name;

      const url = `/${table}?delta_name=eq.${encodeURIComponent(delta)}&start_interval=gte.${encodeURIComponent(startDate)}&end_interval=lte.${encodeURIComponent(endDate)}`;
      console.log("URL para PostgREST:", url);

      try {
        const response = await postgrestFetch(url);
        const data = response['data'] || [];

        const grouped = {};
        data.forEach(item => {
          const key = item.start_interval;
          if (!grouped[key]) grouped[key] = { values: [], counts: [] };
          grouped[key].values.push(item.avg_delta_minutes);
          grouped[key].counts.push(item.count_events);
        });

        const chartData = { labels: [], datasets: [{ label: delta, data: [] }] };

        Object.keys(grouped).sort().forEach(interval => {
          const g = grouped[interval];
          const mean = combinedMean(g.values, g.counts);
          chartData.labels.push(interval);
          chartData.datasets[0].data.push(mean);
        });

        this.chartData = chartData;
        console.log("Chart data preparado:", chartData);
      } catch (err) {
        console.error("Erro ao buscar dados:", err);
      }
    }
  },
  computed: {
    series() {
      const ds = this.chartData.datasets[0];
      return [
        {
          name: ds.label,
          data: ds.data.map((value, i) => ({
            x: this.chartData.labels[i],
            y: value
          }))
        }
      ];
    },
    chartOptions() {
      return {
        chart: { type: 'line', height: 350, zoom: { enabled: false } },
        xaxis: { type: 'category' },
        yaxis: { title: { text: 'Valor médio (minutos)' } },
        stroke: { curve: 'smooth' },
        dataLabels: { enabled: false },
        title: { text: this.selectedNode?.delta_display_name || '', align: 'left' },
        grid: { row: { colors: ['#f3f3f3', 'transparent'], opacity: 0.5 } }
      };
    }
  }
};
</script>

<style scoped>
:deep(.v-slider-thumb__label) {
  font-size: 13px;
  padding: 10px 36px;
  white-space: nowrap;
}
</style>
