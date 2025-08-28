
// Cores do tema
import * as vNG from "v-network-graph"


const initialConfigs = vNG.defineConfigs({
    view: {
    scalingObjects: true,
    minZoomLevel: 1.5,
    maxZoomLevel: 16,                    // desativa zoom
    autoPanAndZoomOnLoad: "center-content",
    mouseWheelZoomEnabled: false,
    panEnabled: false
    // autoPanOnResize: true
  },
  node: {
    selectable: true,
    normal: {
      type: "rect", // Retângulo para bordas quadradas
      width: 64,
      height: 36,
      borderRadius: 6, // Borda arredondada, altere para 0 para borda quadrada
      strokeWidth: 2,
      strokeColor: "rgb(var(--v-theme-secondary))", // Cor do stroke (bordas) = secondary color do Vuetify
      strokeDasharray: "0",
      color: "rgba(255, 255, 255, 1)", // Cor do nó
    },
    hover: {
      type: "rect", // Retângulo para bordas quadradas
      width: 64,
       height: 36,
      borderRadius: 6, // Borda arredondada, altere para 0 para borda quadrada
      strokeWidth: 4,
      strokeColor: "rgb(var(--v-theme-secondary))", // Cor do stroke (bordas) = secondary color do Vuetify
      strokeDasharray: "0",
      color: "rgb(var(--v-theme-secondary-light))", // Cor do nó
    },
    selected: {
      type: "rect", // Retângulo para bordas quadradas
      width: 64,
       height: 36,
      borderRadius: 6, // Borda arredondada, altere para 0 para borda quadrada
      strokeWidth: 3,
      strokeColor: "rgb(var(--v-theme-secondary))", // Cor do stroke (bordas) = secondary color do Vuetify
      strokeDasharray: "0",
      color: "rgb(var(--v-theme-secondary-light))", // Cor do nó
    },
    label: {
      visible: true,
      fontFamily: undefined,
      fontSize: 10,
      lineHeight: 1.1,
      color: "#000000",
      margin: 4,
      direction: "center",
      background: {
        visible: false,
        color: "#ffffff",
        padding: {
          vertical: 1,
          horizontal: 4,
        },
        borderRadius: 2,
      },
    },
    focusring: {
       visible: false,
      width: 4,
      padding: 3,
      color: "#eebb00",
      dasharray: "0",
    },
  },
  edge: {
    selectable: true,
    normal: {
      width: 3,
      color: " rgb(var(--v-theme-primary))", // Cor da aresta = primary color do Vuetify
      dasharray: "0",
      linecap: "butt",
      animate: false,
      animationSpeed: 50,
    },
    hover: {
      width: 4,
      color: "rgb(var(--v-theme-primary))", // Cor da aresta = primary color do Vuetify
      dasharray: "0",
      linecap: "butt",
      animate: false,
      animationSpeed: 50,
    },
    selected: {
      width: 3,
      color: "#dd8800",
      dasharray: "6",
      linecap: "round",
      animate: false,
      animationSpeed: 50,
    },
    gap: 3,
    type: "straight",
    summarize: true,
    label: {  // <-- aqui!
      visible: true,
      fontSize: 8, // diminua para 2 ou 4
    },
    summarized: {
      label: {
        fontSize: 2,
        color: "rgb(var(--v-theme-primary))", // Cor do texto da aresta = primary color do Vuetify
      },
      shape: {
        type: "rect",
        radius: 6,
        width: 12,
        height: 12,
        borderRadius: 3,
        color: "#ffffff",
        strokeWidth: 1,
        strokeColor: "rgb(var(--v-theme-primary))", // Cor da borda da forma da aresta
        strokeDasharray: "0",
      },
      stroke: {
        width: 5,
        color: "rgb(var(--v-theme-primary))", // Cor da borda da forma da aresta
        dasharray: "0",
        linecap: "butt",
        animate: false,
        animationSpeed: 50,
      },
    },
  }
});


export default initialConfigs;