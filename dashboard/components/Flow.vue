<template>
 <div>
  <VueFlow
    :nodes="nodes"
    :edges="edges"
    class="basic-flow"
    :default-viewport="{ zoom: 0.8 }"
    :min-zoom="0.2"
    :max-zoom="4"
  />

  </div>
</template>

<script>
import { VueFlow, useVueFlow } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { ControlButton, Controls } from '@vue-flow/controls'
import { MiniMap } from '@vue-flow/minimap'

export default {
  name: 'FlowDiagram',

  components: {
    VueFlow,
    Background,
    Controls,
    ControlButton,
    MiniMap
  },

  data() {
    return {
      nodes: [],
      edges: []
    }
  },

  mounted() {
    fetch('/delta_sequences.json')
      .then(res => res.json())
      .then(data => {
        let flow = this.genFlow(data);
  
        this.nodes = flow['Nodes'];
        this.edges = flow['Edges'];

        console.log(this.nodes)
        console.log(this.edges)


      })
  },

  methods: {
    genFlow(flowData) {
      const nodeMap = new Map()
      const nodes = []
      const edges = []

      for (const item of flowData) {
        if (item.type === 'between') {
          if (!nodeMap.has(item.display_name_start)) {
            nodes.push({
              id: item.display_name_start,
              data: { label: item.display_name_start },
              position: { x: item.start_x || 0, y: item.start_y || 0 },
              class: 'light',
            })
            nodeMap.set(item.display_name_start, true)
          }

          if (!nodeMap.has(item.display_name_end)) {
            nodes.push({
              id: item.display_name_end,
              data: { label: item.display_name_end },
              position: { x: item.end_x || 0, y: item.end_y || 0 },
              class: 'light',
            })
            nodeMap.set(item.display_name_end, true)
          }
          edges.push({
              id: `${item.display_name_start} ${item.display_name_end}`,
              source:  item.display_name_start,
              target: item.display_name_end
          })
        }

        if (item.type === 'box') {
          if(!nodeMap.has(item.display_name)){
            nodes.push({
              id: item.display_name,
              data: { label: item.display_name },
              position: { x: item.start_x || 0, y: item.start_y || 0 },
              class: 'light',
            })
            nodeMap.set(item.display_name, true)
          }
        }
      }

      return {
        'Nodes': nodes,
        'Edges': edges
      }
    }
  }
}
</script>

<style scoped>
.vue-flow-basic {
  width: 100%;
  height: 600px;
  border: 1px solid #ddd;
  border-radius: 12px;
}
</style>
