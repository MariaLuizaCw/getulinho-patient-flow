<template>
  <div class="demo-control-panel">
    <el-tabs type="border-card">
      <el-tab-pane label="Node">
        <el-tabs>
          <el-tab-pane label="normal">
            <demo-node-config-panel v-model:type="configs.node.normal.type" v-model:radius="configs.node.normal.radius"
              v-model:width="configs.node.normal.width" v-model:height="configs.node.normal.height"
              v-model:borderRadius="configs.node.normal.borderRadius"
              v-model:strokeWidth="configs.node.normal.strokeWidth"
              v-model:strokeColor="configs.node.normal.strokeColor"
              v-model:strokeDasharray="configs.node.normal.strokeDasharray" v-model:color="configs.node.normal.color" />
          </el-tab-pane>
          <el-tab-pane label="hover">
            <demo-node-config-panel v-model:type="configs.node.hover.type" v-model:radius="configs.node.hover.radius"
              v-model:width="configs.node.hover.width" v-model:height="configs.node.hover.height"
              v-model:borderRadius="configs.node.hover.borderRadius"
              v-model:strokeWidth="configs.node.hover.strokeWidth" v-model:strokeColor="configs.node.hover.strokeColor"
              v-model:strokeDasharray="configs.node.hover.strokeDasharray" v-model:color="configs.node.hover.color" />
          </el-tab-pane>
          <el-tab-pane label="selected">
            <demo-node-config-panel v-model:type="configs.node.selected.type"
              v-model:radius="configs.node.selected.radius" v-model:width="configs.node.selected.width"
              v-model:height="configs.node.selected.height" v-model:borderRadius="configs.node.selected.borderRadius"
              v-model:strokeWidth="configs.node.selected.strokeWidth"
              v-model:strokeColor="configs.node.selected.strokeColor"
              v-model:strokeDasharray="configs.node.selected.strokeDasharray"
              v-model:color="configs.node.selected.color" />
          </el-tab-pane>
        </el-tabs>
      </el-tab-pane>
      <el-tab-pane label="Label">
        <el-tabs>
          <el-tab-pane label="basic">
            <demo-label-config-panel v-model:visible="configs.node.label.visible"
              v-model:fontFamily="configs.node.label.fontFamily" v-model:fontSize="configs.node.label.fontSize"
              v-model:lineHeight="configs.node.label.lineHeight" v-model:color="configs.node.label.color"
              v-model:margin="configs.node.label.margin" v-model:direction="configs.node.label.direction" />
          </el-tab-pane>
          <el-tab-pane label="background">
            <demo-label-background-config-panel v-model:visible="configs.node.label.background.visible"
              v-model:color="configs.node.label.background.color"
              v-model:paddingVertical="configs.node.label.background.padding.vertical" v-model:paddingHorizontal="configs.node.label.background.padding.horizontal
                " v-model:borderRadius="configs.node.label.background.borderRadius" />
          </el-tab-pane>
        </el-tabs>
      </el-tab-pane>
      <el-tab-pane label="Focus">
        <demo-focus-config-panel v-model:visible="configs.node.focusring.visible"
          v-model:width="configs.node.focusring.width" v-model:padding="configs.node.focusring.padding"
          v-model:color="configs.node.focusring.color" v-model:dasharray="configs.node.focusring.dasharray" />
      </el-tab-pane>
      <el-tab-pane label="Edge">
        <el-tabs>
          <el-tab-pane label="normal">
            <demo-edge-config-panel v-model:width="configs.edge.normal.width" v-model:color="configs.edge.normal.color"
              v-model:dasharray="configs.edge.normal.dasharray" v-model:linecap="configs.edge.normal.linecap"
              v-model:animate="configs.edge.normal.animate"
              v-model:animationSpeed="configs.edge.normal.animationSpeed" />
          </el-tab-pane>
          <el-tab-pane label="hover">
            <demo-edge-config-panel v-model:width="configs.edge.hover.width" v-model:color="configs.edge.hover.color"
              v-model:dasharray="configs.edge.hover.dasharray" v-model:linecap="configs.edge.hover.linecap"
              v-model:animate="configs.edge.hover.animate" v-model:animationSpeed="configs.edge.hover.animationSpeed" />
          </el-tab-pane>
          <el-tab-pane label="selected">
            <demo-edge-config-panel v-model:width="configs.edge.selected.width"
              v-model:color="configs.edge.selected.color" v-model:dasharray="configs.edge.selected.dasharray"
              v-model:linecap="configs.edge.selected.linecap" v-model:animate="configs.edge.hover.animate"
              v-model:animationSpeed="configs.edge.hover.animationSpeed" />
          </el-tab-pane>
          <el-tab-pane label="multiple edge">
            <demo-multiple-edge-config-panel v-model:gap="configs.edge.gap" v-model:line-type="configs.edge.type"
              v-model:summarize="configs.edge.summarize" />
          </el-tab-pane>
        </el-tabs>
      </el-tab-pane>
      <el-tab-pane label="Summarized Edge">
        <el-tabs>
          <el-tab-pane label="stroke">
            <demo-edge-config-panel v-model:width="configs.edge.summarized.stroke.width"
              v-model:color="configs.edge.summarized.stroke.color"
              v-model:dasharray="configs.edge.summarized.stroke.dasharray"
              v-model:linecap="configs.edge.summarized.stroke.linecap"
              v-model:animate="configs.edge.summarized.stroke.animate"
              v-model:animationSpeed="configs.edge.summarized.stroke.animationSpeed" />
          </el-tab-pane>
          <el-tab-pane label="label">
            <demo-summarized-edge-label-config-panel v-model:labelFontSize="configs.edge.summarized.label.fontSize"
              v-model:labelColor="configs.edge.summarized.label.color" v-model:type="configs.edge.summarized.shape.type"
              v-model:radius="configs.edge.summarized.shape.radius" v-model:width="configs.edge.summarized.shape.width"
              v-model:height="configs.edge.summarized.shape.height"
              v-model:borderRadius="configs.edge.summarized.shape.borderRadius"
              v-model:strokeWidth="configs.edge.summarized.shape.strokeWidth"
              v-model:strokeColor="configs.edge.summarized.shape.strokeColor"
              v-model:strokeDasharray="configs.edge.summarized.shape.strokeDasharray"
              v-model:color="configs.edge.summarized.shape.color" />
          </el-tab-pane>
        </el-tabs>
      </el-tab-pane>
    </el-tabs>
  </div>

  <v-network-graph   :event-handlers="eventHandlers" :nodes="nodes" :edges="edges" :layouts="layouts" :configs="configs" class="graph-container">

    <template #override-node-label="{
      nodeId, scale, text, x, y, config, textAnchor, dominantBaseline
    }">
      <!-- Label acima do nó -->
      <text :x="x" :y="y - 25" :font-size="9 * scale" text-anchor="middle" dominant-baseline="central" fill="#000000">
        {{ nodes[nodeId].labelAbove }} 
      </text> <!-- Informações acima do nó -->

      <!-- Label centralizado (padrão) -->
      <text v-if="nodes[nodeId].name.includes('\n')" :x="x" :y="y + config.fontSize * 0.6 * scale"
        :font-size="config.fontSize * scale" :text-anchor="textAnchor" dominant-baseline="middle" fill="#000000">
        <tspan v-for="(line, index) in (lines = nodes[nodeId].name.split('\n'))" :key="index" :x="x"
          :dy="(index - (lines.length - 1) / 2) * config.fontSize * 2 * scale">
          {{ line }}
        </tspan>
      </text>

      <!-- Texto sem quebra de linha -->
      <text v-else :x="x" :y="y" :font-size="config.fontSize * scale" :text-anchor="textAnchor"
        dominant-baseline="middle" fill="#000000">
        {{ nodes[nodeId].name }}
      </text>
      <!-- Label abaixo do nó -->
      <text :x="x" :y="y + 25" :font-size="9 * scale" text-anchor="middle" dominant-baseline="central" fill="#000000">
        {{ nodes[nodeId].labelBelow }} 
      </text> <!-- Informações abaixo do nó -->
    </template>

    <template #edge-label="{ edge, ...slotProps }">
      <!-- Label acima da aresta -->
      <v-edge-label :text="edge.labelAbove" align="center" vertical-align="above" v-bind="slotProps" />

      <!-- Label abaixo da aresta -->
      <v-edge-label :text="edge.labelBelow" align="center" vertical-align="below" v-bind="slotProps" />
    </template>
  </v-network-graph>
</template>

<script>

// Cores do tema
import * as vNG from "v-network-graph"
import { reactive } from 'vue'  // Usando reactive
import "v-network-graph/lib/style.css"
import initialConfigs from '../config/networkConfigs'
import { calculateEdgeTop, calculateEdgeBottom } from '~/utils/flow_functions';


export default {
  name: 'FlowDiagram',
  props: {
    deltas: {
      type: Array,
      default: () => []
    }
  },

  setup() {
    const configs = reactive(initialConfigs) // Torna o objeto reativo
    return { configs }
  },

  data() {
    return {
      nodes: {
        node1: { name: "Node 1", labelAbove: 'teste', labelBelow: 'teste2' },
        node2: { name: "Node 2", labelAbove: 'teste', labelBelow: 'teste2' },
        node3: { name: "Node 3", labelAbove: 'teste', labelBelow: 'teste2' },
        node4: { name: "Node 4", labelAbove: 'teste', labelBelow: 'teste2' },
      },
      edges: {
        edge1: { source: "node1", target: "node2", labelAbove: 'teste', labelBelow: 'teste2' },
        edge2: { source: "node2", target: "node3", labelAbove: 'teste', labelBelow: 'teste2' },
        edge3: { source: "node3", target: "node4", labelAbove: 'teste', labelBelow: 'teste2' },
      },
      layouts: {
        nodes: {
          node1: { x: 0, y: 0 },         // Posição do Node 1
          node2: { x: 50, y: 50 },       // Posição do Node 2
          node3: { x: 100, y: 0 },       // Posição do Node 3
          node4: { x: 150, y: 50 },      // Posição do Node 4
        }
      }
    }
  },
  mounted() {
    fetch('/delta_sequences.json')
      .then(res => res.json())
      .then(flowData => {
        // Passa o deltas que vem do FiltersPanel
        const flow = this.genFlow(flowData, this.deltas || []);
        
        this.nodes = flow.nodes;
        this.edges = flow.edges;
        this.layouts = flow.layouts;


        
      });
  },
  computed: {
    eventHandlers() {
      return {
        "node:click": ({ node }) => {
          const nodeData = this.nodes[node]     // pega os dados do nó
          const originalItem = nodeData.raw     // aqui tá o objeto do JSON
          console.log("🔵 Nó clicado:", originalItem)
          this.$emit("node-clicked", originalItem)
        },
        "edge:click": ({ edge }) => {
          const edgeData = this.edges[edge]
          console.log("🟠 Aresta clicada:", edgeData.raw || edgeData)
          this.$emit("edge-clicked", edgeData.raw || edgeData)
        }
      }
    }
  },
  watch: {
    deltas(newDeltas) {
      if (!newDeltas) return;
      fetch('/delta_sequences.json')
        .then(res => res.json())
        .then(flowData => {
          const flow = this.genFlow(flowData, newDeltas);
          this.nodes = flow.nodes;
          this.edges = flow.edges;
          this.layouts = flow.layouts;
        });
    }
  },
  methods: {
    wrapText(text, maxCharsPerLine = 10) {
      const words = text.split(" ");
      let lines = [];
      let currentLine = "";

      for (const word of words) {
        if ((currentLine + word).length > maxCharsPerLine) {
          lines.push(currentLine.trim());
          currentLine = "";
        }
        currentLine += word + " ";
      }
      if (currentLine) lines.push(currentLine.trim());
      return lines.join("\n");
    },
    
    genFlow(flowData, deltas) {
      const nodeMap = new Map();
      const nodes = {};
      const edges = {};
      const layouts = { nodes: {} };
      console.log('do lado do flow:', deltas)
      for (const item of flowData) {
        if (item.type === 'box') {
          // Se já existe, deleta antes
          if (nodeMap.has(item.display_name)) {
            delete nodes[item.display_name];
            delete layouts.nodes[item.display_name];
            nodeMap.delete(item.display_name);
          }

          // Cria/atualiza o nó
          nodes[item.display_name] = {
            name: item.display_name,
            labelAbove: calculateEdgeTop(deltas, item.delta_name),
            labelBelow: calculateEdgeBottom(deltas, item.delta_name),
            raw: item
          };
          layouts.nodes[item.display_name] = {
            x: item.start_x || 0,
            y: item.start_y || 0
          };
          nodeMap.set(item.display_name, true);
        }
        if (item.type === 'between') {
          if (!nodeMap.has(item.display_name_start)) {
            nodes[item.display_name_start] = {
              name: item.display_name_start,
              labelAbove: '',
              labelBelow: '',
              raw: item
            };
            layouts.nodes[item.display_name_start] = {
              x: item.start_x || 0,
              y: item.start_y || 0
            };
            nodeMap.set(item.display_name_start, true);
          }

          if (!nodeMap.has(item.display_name_end)) {
            nodes[item.display_name_end] = {
              name: item.display_name_end,
              labelAbove: '',
              labelBelow: '',
            };
            layouts.nodes[item.display_name_end] = {
              x: item.end_x || 0,
              y: item.end_y || 0
            };
            nodeMap.set(item.display_name_end, true);
          }

          edges[`${JSON.stringify(item)}`] = {
            source: item.display_name_start,
            target: item.display_name_end,
            labelAbove: calculateEdgeTop(deltas, item.delta_name),
            labelBelow: calculateEdgeBottom(deltas, item.delta_name),
            raw: item
          };
        }

        
      }

      return { nodes, edges, layouts };
    }
  }
}
</script>

<style scoped>
.graph-container {
  width: 100%;
  /* Ocupa toda a largura disponível */
  height: 360px;
  /* Ajuste a altura para o valor desejado, ex: 800px */
}
</style>
