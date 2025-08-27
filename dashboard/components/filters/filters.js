// filters/filters.js
import { postgrestFetch } from '~/utils/postgrestFetch.js';


export const timeFilters = {};

// Função para registrar filtros
export function registerTimeFilter(name, fn) {
  timeFilters[name] = fn;
}

export async function getLastReportTime(timeTable) {
  try {
    const response = await postgrestFetch(`/last_update_log?table_name=eq.${timeTable}`);

    if (response._status === 200) {
      return response.data[0].report_time; // já é o JSON retornado pelo PostgREST
    } else {
      console.error('Error fetching last report time');
      return null;
    }

  } catch (error) {
    console.error('Error fetching last report time:', error);
    return null;
  }
}


// Filtro para as últimas 4 horas
export async function filterLastXHours(queryParams, timeTable, timeComplement) {
  const lastReportTime = await getLastReportTime(timeTable);
  if (lastReportTime) {
    queryParams.append('start_interval', `gte.${lastReportTime}`);
  }
}



// Filtro para um dia específico
export async function filterSpecificDay(queryParams, timeTable, timeComplement) {
  const { day } = timeComplement; // ex: "2025-08-26"
  console.log(day)
  queryParams.append('start_interval', `eq.${day}`);
}
// Filtro para um mês específico
export async function filterSpecificMonth(queryParams, timeTable, timeComplement) {
  const { year, month } = timeComplement;
  const monthNumber = getMonthNumber(month);
  if (year && monthNumber) {
    const start_dt = new Date(year, monthNumber - 1, 1);
    const end_dt = new Date(year, monthNumber, 1);
    queryParams.append('start_interval', `gte.${start_dt.toISOString()}`);
    queryParams.append('start_interval', `lt.${end_dt.toISOString()}`);
  }
}

function getMonthNumber(monthName) {
  const monthMapping = {
    "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4,
    "Maio": 5, "Junho": 6, "Julho": 7, "Agosto": 8,
    "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12
  };
  return monthMapping[monthName];
}

// Registra os filtros de tempo
export function registerTimeFilters() {
  registerTimeFilter('4 horas', filterLastXHours);
  registerTimeFilter('24 horas', filterLastXHours);
  registerTimeFilter('Dia', filterSpecificDay);
  registerTimeFilter('Mês', filterSpecificMonth);
}
