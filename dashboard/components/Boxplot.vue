<template>
   <v-container>
    <h3>Distribuição por Classe de Risco</h3>
    <div class="text-subtitle-1" v-if="flowData?.length">
      Intervalo da distribuição: 
      {{ formatDateTime(flowData[0].start_interval) }} - 
      {{ formatDateTime(flowData[0].end_interval) }}
    </div>
     <div style="height: 30px;"></div>
    <apexchart 
      type="boxPlot" 
      :options="chartOptions" 
      :series="chartSeries" 
      height="350"
    />
   </v-container>
</template>

<script>
import { formatDateTime } from '~/utils/flow_functions';

export default {
  props: {
    flowData: {
      type: Array,
      required: true
    },
    selectedNode: {
      type: Object,
      required: true
    }
  },
  data() {
    return {
      chartSeries: [],
      chartOptions: {
        chart: {
          type: 'boxPlot',
          toolbar: { show: true },
        },
        plotOptions: {
          boxPlot: {
            colors: {
              upward: '#00E396',   // default se não usar mapeamento
              downward: '#FF4560'
            }
          }
        },
        title: {
          text: '',
          align: 'left'
        },
        xaxis: {
          type: 'category'
        },
        yaxis: {
          labels: {
            formatter: val => val.toFixed(2) // duas casas decimais
          }
        },
        tooltip: {
          shared: true,
          y: {
            formatter: val => val.toFixed(2)
          }
        }
      },
      riskColors: {
        AMARELO: '#FFC107',
        AZUL: '#007BFF',
        VERDE: '#28A745',
        VERMELHO: '#DC3545'
      }
    };
  },
  watch: {
    flowData: 'updateChart',
    selectedNode: 'updateChart'
  },
  mounted() {
    this.updateChart();
  },
  methods: {
    updateChart() {
      if (!this.flowData || !this.selectedNode) return;

      // Filtra pelo delta_name
      const filtered = this.flowData.filter(d => d.delta_name === this.selectedNode.delta_name);

      // Monta os dados do boxplot
      const data = filtered
        .filter(item => item.min_delta_minutes && item.max_delta_minutes)
        .map(item => ({
          x: item.risk_class,
          y: [
            parseFloat(item.min_delta_minutes.toFixed(2)),
            parseFloat(item.p25_delta_minutes.toFixed(2)),
            parseFloat(item.p50_delta_minutes.toFixed(2)),
            parseFloat(item.p75_delta_minutes.toFixed(2)),
            parseFloat(item.max_delta_minutes.toFixed(2))
          ],
          fillColor: this.riskColors[item.risk_class] || '#000000' // cor específica
        }));

      // Monta a série do ApexCharts (uma única série)
      this.chartSeries = [{ data }];
        this.$nextTick(() => {
        window.dispatchEvent(new Event('resize'));
        });
    }
  },
};
</script>
