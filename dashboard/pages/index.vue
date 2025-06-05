<template>
  <v-container>
    <v-container>
        <div class="text-h4 font-weight-black">
        Lasos FluxoSaúde: Painel de Monitoramento Inteligente do Fluxo Hospitalar
    </div>

    <div class="text-subtitle-1 mt-4">
        Dados entre
        <span class="text-primary font-weight-medium">2025-05-30 17:00:00</span>
        e
        <span class="text-primary font-weight-medium">2025-05-30 21:00:00</span>
    </div>
    </v-container>
    <v-container>
        <FiltersPanel />
        
    </v-container>
    <v-container>
        <Flow />
    </v-container>
  </v-container>
</template>
