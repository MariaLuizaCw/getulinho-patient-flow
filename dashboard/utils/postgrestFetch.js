// utils/postgrestFetch.js
export async function postgrestFetch(path, options = {}) {
  const config = useRuntimeConfig();
  const url = `${config.public.postgrestBaseUrl}${path}`;

  try {
    let status = 200;

    const data = await $fetch(url, {
      ...options,
      responseType: 'json',
      onResponse({ response }) {
        status = response.status; // captura o status aqui
      }
    });

    return { _status: status, data }; // retorna status + dados

  } catch (error) {
    return {
      _status: error.response?.status || 500,
      _error: error.message
    };
  }
}
