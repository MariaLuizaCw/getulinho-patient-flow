// filters/filters.js

export const timeFilters = {};

// Função para registrar filtros
export function registerTimeFilter(name, fn) {
  timeFilters[name] = fn;
}

// Funções de filtro específicas
export async function getLastReportTime(timeTable) {
  try {
    const response = await fetch(`/get_last_report_time?table_name=${timeTable}`);
    if (response.ok) {
      return await response.json(); // Retorna o tempo do último relatório
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
export async function filterLast4Hours(queryParams, timeTable) {
  const lastReportTime = await getLastReportTime(timeTable);
  if (lastReportTime) {
    const fourHoursAgo = new Date(lastReportTime - (4 * 60 * 60 * 1000));
    queryParams.append('start_interval', `gte.${fourHoursAgo.toISOString()}`);
  }
}

// Filtro para as últimas 24 horas
export async function filterLast24Hours(queryParams, timeTable) {
  const lastReportTime = await getLastReportTime(timeTable);
  if (lastReportTime) {
    const twentyFourHoursAgo = new Date(lastReportTime - (24 * 60 * 60 * 1000));
    queryParams.append('start_interval', `gte.${twentyFourHoursAgo.toISOString()}`);
  }
}

// Filtro para um dia específico
export async function filterSpecificDay(queryParams, timeComplement, timeTable) {
  const { day } = timeComplement;
  const start_dt = new Date(day);
  const end_dt = new Date(start_dt);
  end_dt.setDate(start_dt.getDate() + 1);

  queryParams.append('start_interval', `gte.${start_dt.toISOString()}`);
  queryParams.append('start_interval', `lt.${end_dt.toISOString()}`);
}

// Filtro para um mês específico
export async function filterSpecificMonth(queryParams, timeComplement, timeTable) {
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
  registerTimeFilter('Últimas 4 horas', filterLast4Hours);
  registerTimeFilter('Últimas 24 horas', filterLast24Hours);
  registerTimeFilter('Selecionar um dia', filterSpecificDay);
  registerTimeFilter('Selecionar um mês', filterSpecificMonth);
}
