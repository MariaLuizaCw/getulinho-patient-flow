import fs from 'fs'


export default defineNuxtConfig({
  compatibilityDate: '2025-05-15',
  devtools: { enabled: true },
  css: ['vuetify/styles', '@/assets/main.css', '@mdi/font/css/materialdesignicons.css', "v-network-graph/lib/style.css"],
  build: {
    transpile: ['vuetify'],
  },
  devServer: {
    port: 5001,
    host: '0.0.0.0', // opcional, se quiser aceitar requisições externas
  },
  runtimeConfig: {
    public: {
      postgrestBaseUrl: process.env.POSTGREST_URL || 'http://postgrest:3000', // URL base do PostgREST
    },
  },
   

})