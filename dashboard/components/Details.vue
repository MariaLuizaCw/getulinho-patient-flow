<template>
  <div>
    <div class="d-flex flex-column justify-space-between" style="height: 400px; gap: 16px;">
      
      <!-- Card do total de eventos -->
      <v-card class="pa-1" elevated height="180">
        <v-card-title class="title-break text-h6">
          {{ deltaDisplayName }}
        </v-card-title>
        <v-card-subtitle class="text-h7 title-break">
          Total de Eventos: {{ totalEvents }}
        </v-card-subtitle>
      </v-card>

      <!-- Card countStart -->
      <v-card v-if="countStart && Object.keys(countStart).length" class="pa-2" outlined height="180">
        <v-card-title class="text-h6 title-break">Detalhamento {{ selectedNode.delta_display_start }}</v-card-title>
        <v-card-subtitle class="text-h7 title-break">
          <div v-for="(count, key) in topCountStart" :key="key">
            {{ key }}: {{ count }}
          </div>
        </v-card-subtitle>
        <v-card-actions v-if="Object.keys(countStart).length > 3">
          
          <v-btn text small @click="showModal('start')">Ver todos</v-btn>
        </v-card-actions>
      </v-card>

      <!-- Card countEnd -->
      <v-card v-if="countEnd && Object.keys(countEnd).length" class="pa-2" outlined height="180">
        <v-card-title class="text-h6 title-break">Detalhamento {{ selectedNode.delta_display_end }}</v-card-title>
        <v-card-subtitle class="text-h7 title-break">
          <div v-for="(count, key) in topCountEnd" :key="key">
            {{ key }}: {{ count }}
          </div>
        </v-card-subtitle>
        <v-card-actions v-if="Object.keys(countEnd).length > 3">
          <v-btn text small @click="showModal('end')">Ver todos</v-btn>
        </v-card-actions>
      </v-card>
    </div>

    <!-- Modal -->
    <v-dialog v-model="modalVisible" max-width="700">
      <v-card>
        <v-card-title class="text-h6 title-break">Detalhes completos</v-card-title>
        <v-card-subtitle class="text-h7 title-break">
          <div v-for="(count, key) in modalData" :key="key">
            {{ key }}: {{ count }}
          </div>
        </v-card-subtitle>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn text @click="modalVisible = false">Fechar</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script>
export default {
  props: {
    selectedNode: { type: Object, required: true },
    flowData: { type: Array, required: true }
  },
  data() {
    return {
      modalVisible: false,
      modalData: {}
    };
  },
  computed: {
    deltaDisplayName() {
      return this.selectedNode?.delta_display_name || 'N/A';
    },
    totalEvents() {
      if (!this.flowData || !this.flowData.length || !this.selectedNode) return 0;
      return this.flowData
        .filter(item => item.delta_name === this.selectedNode.delta_name)
        .reduce((sum, item) => sum + (item.count_events || 0), 0);
    },
    // Contagens filtradas > 0
    countStart() {
      return this.aggregateCounts('spare1_start_count_json');
    },
    countEnd() {
      return this.aggregateCounts('spare1_end_count_json');
    },
    // Top 3 para exibição
    topCountStart() {
      return this.topCounts(this.countStart);
    },
    topCountEnd() {
      return this.topCounts(this.countEnd);
    }
  },
  methods: {
    aggregateCounts(field) {
      if (!this.selectedNode || !this.flowData?.length) return null;
      const filtered = this.flowData.filter(item => item.delta_name === this.selectedNode.delta_name);
      const total = {};
      filtered.forEach(item => {
        if (!item[field]) return;
        const data = JSON.parse(item[field]);
        for (const key in data) {
          total[key] = (total[key] || 0) + data[key];
        }
      });
      const filteredTotal = {};
      for (const key in total) {
        if (total[key] > 0) filteredTotal[key] = total[key];
      }
      return Object.keys(filteredTotal).length ? filteredTotal : null;
    },
    topCounts(counts) {
      if (!counts) return {};
      return Object.fromEntries(
        Object.entries(counts)
          .sort((a, b) => b[1] - a[1])
          .slice(0, 3)
      );
    },
    showModal(type) {
      if (type === 'start') this.modalData = this.countStart;
      else if (type === 'end') this.modalData = this.countEnd;
      this.modalVisible = true;
    }
  }
};
</script>

<style scoped>
.v-card {
  max-width: 300px;
}
.title-break {
  white-space: pre-wrap;
  word-wrap: break-word;
  overflow-wrap: break-word;
}
</style>
