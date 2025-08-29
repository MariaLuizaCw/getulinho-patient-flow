// flow_functions.js
export function combinedMean(values, counts) {
    let totalWeighted = 0;
    let totalCount = 0;
    for (let i = 0; i < values.length; i++) {
        totalWeighted += values[i] * counts[i];
        totalCount += counts[i];
    }
    return totalCount === 0 ? null : totalWeighted / totalCount;
}

export function combinedStd(values, stds, counts) {
    const mean = combinedMean(values, counts);
    if (mean === null) return null;
    let totalVariance = 0;
    let totalCount = 0;
    for (let i = 0; i < values.length; i++) {
        totalVariance += (Math.pow(stds[i], 2) + Math.pow(values[i] - mean, 2)) * counts[i];
        totalCount += counts[i];
    }
    return totalCount === 0 ? null : Math.sqrt(totalVariance / totalCount);
}

export function calculateEdgeTop(deltaArray, deltaName) {
    if (!deltaArray || deltaArray.length === 0) return "sem dados";
    const filtered = deltaArray.filter(d => d.delta_name === deltaName);
    if (filtered.length === 0) return "sem dados";
    const values = filtered.map(d => d.avg_delta_minutes);
    const stds = filtered.map(d => d.stddev_delta_minutes);
    const counts = filtered.map(d => d.count_events);
    const mean = combinedMean(values, counts);
    const std = combinedStd(values, stds, counts);
    if (mean === null || std === null) return "sem dados";
    return `${mean.toFixed(1)} ± ${std.toFixed(1)} min`;
}

export function calculateEdgeBottom(deltaArray, deltaName) {
    if (!deltaArray || deltaArray.length === 0) return "sem dados";
    const filtered = deltaArray.filter(d => d.delta_name === deltaName);
    if (filtered.length === 0) return "sem dados";
    const totalEvents = filtered.reduce((sum, d) => sum + (d.count_events || 0), 0);
    return totalEvents === 0 ? "sem dados" : `${totalEvents} eventos`;
}


export function formatDateTime(value) {
    if (!value) return "";
    const date = typeof value === "string" ? new Date(value) : value;

    const pad = (n) => String(n).padStart(2, "0");

    const year = date.getFullYear();
    const month = pad(date.getMonth() + 1);
    const day = pad(date.getDate());
    const hours = pad(date.getHours());
    const minutes = pad(date.getMinutes());
    const seconds = pad(date.getSeconds());

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}