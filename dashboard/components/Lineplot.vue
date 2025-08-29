<template>
  <v-container>
   <h3>Evolução do Tempo</h3>
    <label>Selecione o intervalo de datas:</label>
    <v-range-slider
      v-model="range"
      :min="0"
      :max="dateOptions.length - 1"
      :step="1"
      thumb-label
      color="primary"
      @end="fetchData"
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
  </v-container>
 


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
      chartData: { labels: [], datasets: [{ label: '', data: [] }] },
      config: {
        '4 horas': { unit: 'day', count: 3 },
        '24 horas': { unit: 'day', count: 3 },
        'Dia': { unit: 'day', count: 60 },
        'Mês': { unit: 'month', count: 12 },
        'Ano': { unit: 'year', count: 5 }
      }
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

      const setting = this.config[timeRange];
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

    formatInterval(start, end) {
      const s = new Date(start);
      const e = new Date(end);
      const pad = n => n.toString().padStart(2, '0');

      const sameDay = s.getDate() === e.getDate() &&
                      s.getMonth() === e.getMonth() &&
                      s.getFullYear() === e.getFullYear();

      if (sameDay) {
        return `${pad(s.getDate())}/${pad(s.getMonth()+1)} ${pad(s.getHours())}:00-${pad(e.getHours())}:00`;
      } else {
        return `${pad(s.getDate())}/${pad(s.getMonth()+1)} ${pad(s.getHours())}:00 - ${pad(e.getDate())}/${pad(e.getMonth()+1)} ${pad(e.getHours())}:00`;
      }
    },

    async fetchData() {
      if (!this.selectedNode || !this.filters) return;
      let startDate, endDate;
      const setting = this.config[this.filters.time_range];
      if (setting.unit === 'month') {
        // Supondo que dateOptions tenha o formato "YYYY-MM"
            const [startYear, startMonth] = this.dateOptions[this.range[0]].split('-');
            const [endYear, endMonth] = this.dateOptions[this.range[1]].split('-');

            // Primeiro dia do mês inicial às 00:00
            startDate = `${startYear}-${startMonth}-01T00:00:00`;

            // Último dia do mês final às 23:59:59
            const lastDay = new Date(endYear, parseInt(endMonth, 10), 0).getDate();
            endDate = `${endYear}-${endMonth}-${String(lastDay).padStart(2, '0')}T23:59:59`;
                

      } else if (setting.unit === 'year') {
        console.log('year')

      } else {
        // Intervalo diário ou outro
        startDate = this.dateOptions[this.range[0]] + 'T00:00:00';
        endDate = this.dateOptions[this.range[1]] + 'T23:59:59';
      }
      const table = this.filters.time_table;
      const delta = this.selectedNode.delta_name;

      let url = `/${table}?`;
      const params = [];

      // filtro delta
      params.push(`delta_name=eq.${encodeURIComponent(delta)}`);
      params.push(`start_interval=gte.${encodeURIComponent(startDate)}`);
      params.push(`end_interval=lte.${encodeURIComponent(endDate)}`);

      // filtro riscos (em uppercase)
      if (this.filters.risks && this.filters.risks.length) {
        const riskParams = this.filters.risks.map(r => encodeURIComponent(r.toUpperCase())).join(',');
        params.push(`risk_class=in.(${riskParams})`);
      }

      // junta tudo
      url += params.join('&');


      console.log("URL para PostgREST:", url);
      try {
        const response = await postgrestFetch(url);
        const data = response['data'] || [];

        const grouped = {};
        data.forEach(item => {
          const key = `${item.start_interval}_${item.end_interval}`;
          if (!grouped[key]) {
            grouped[key] = { 
              start: item.start_interval, 
              end: item.end_interval, 
              values: [], 
              counts: [] 
            };
          }
          grouped[key].values.push(item.avg_delta_minutes);
          grouped[key].counts.push(item.count_events);
        });

          const chartData = {
          labels: [],
          seriesDelta: [],   // média do tempo
          seriesCount: []    // contagem de pessoas
        };

        Object.values(grouped)
          .sort((a, b) => new Date(a.start) - new Date(b.start))
          .forEach(g => {
            chartData.labels.push(this.formatInterval(g.start, g.end));
            chartData.seriesDelta.push(combinedMean(g.values, g.counts));
            chartData.seriesCount.push(g.counts.reduce((a,b) => a+b, 0)); // soma total de pessoas
          });

        this.chartData = chartData;

        console.log("Chart data preparado:", chartData);
      } catch (err) {
        console.error("Erro ao buscar dados:", err);
      }
    }
  },
    watch: {
    selectedNode() {
      this.fetchData();
    },
    filters: {
      handler() {
        this.generateDateOptions(this.filters.time_range);
        this.range = [0, this.dateOptions.length - 1];
        this.fetchData();
      },
      deep: true
    }
  },
  computed: {
   series() {
    return [
      {
        name: 'Tempo médio (minutos)',
        type: 'line',
        data: this.chartData.seriesDelta
      },
      {
        name: 'Contagem de pessoas',
        type: 'line', // ou 'column' se quiser barras
        data: this.chartData.seriesCount
      }
    ];
  },
  chartOptions() {
    return {
      chart: { type: 'line', height: 350, stacked: false },
      stroke: { width: [3, 3] },
      xaxis: {
        categories: this.chartData.labels,
        labels: { rotate: -45 }
      },
      yaxis: [
        { 
          title: { text: 'Tempo médio (minutos)' },
          labels: { formatter: val => val.toFixed(2) }
        },
        { 
          opposite: true, // eixo direito
          title: { text: 'Contagem de pessoas' },
          labels: { formatter: val => val.toFixed(0) }
        }
      ],
      tooltip: { shared: true },
      dataLabels: { enabled: false },
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
