import { createVuetify } from 'vuetify'
import { pt } from 'vuetify/locale'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'
import { VDateInput } from 'vuetify/labs/VDateInput'
import 'vuetify/styles'
import '@mdi/font/css/materialdesignicons.css'

export default defineNuxtPlugin((nuxtApp) => {
  const vuetify = createVuetify({
    components: {
      ...components,
      VDateInput 
    },
    directives,
    ssr: true,
    locale: {
      locale: 'pt',
      messages: { pt }
    },
    theme: {
      defaultTheme: 'mainTheme',
      themes: {
        mainTheme: {
          dark: false,
          colors: {
            background: '#FFFFFF',
            surface: '#F5F5F5',
            primary: '#ef731b',
            'primary-light': '#ffe5b4',
            secondary: '#007BFF',
            'secondary-light': '#E6F0FF',
            text: '#000000',
            error: '#D84315',
            success: '#4CAF50',
            warning: '#FB8C00',
            info: '#2196F3'
          }
        }
      }
    },
  })

  nuxtApp.vueApp.use(vuetify)
})
