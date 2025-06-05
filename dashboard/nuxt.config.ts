// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  compatibilityDate: '2025-05-15',
  devtools: { enabled: true },
  css: ['vuetify/styles', '@/assets/main.css', '@mdi/font/css/materialdesignicons.css'],
  build: {
    transpile: ['vuetify'],
  },
  devServer: {
    port: 5001,
    host: '0.0.0.0', // opcional, se quiser aceitar requisições externas
  }
})