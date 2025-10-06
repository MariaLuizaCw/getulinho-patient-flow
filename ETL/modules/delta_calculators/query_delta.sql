WITH risk_class AS (
    SELECT 
        a.pacienteid,
        COALESCE(NULLIF(b.risk_class, ''), 'INDEFINIDO') AS risk_class,  -- Caso não haja risk_class, será 'Indefinido'
        b.date,
        ROW_NUMBER() OVER (
            PARTITION BY a.pacienteid, a.event_name, a.real_date
            ORDER BY ABS(EXTRACT(EPOCH FROM a.real_date - b.date))  -- Menor diferença entre as datas
        ) AS rn,
        a.real_date,
        a.event_name,
        a.spare1,
        a.spare2
    FROM public.events a
    LEFT JOIN public.last_risk_class b
        ON a.pacienteid = b.pacienteid
    WHERE a.real_date BETWEEN ':start_interval'::timestamp WITHOUT time zone 
                          AND ':end_interval'::timestamp WITHOUT time zone
), lags AS (
    SELECT 
        a.pacienteid,
        a.event_name AS event_name,
        a.real_date AS real_date,
        LAG(a.event_name, :lag_n) OVER (PARTITION BY a.pacienteid ORDER BY a.real_date) AS previous_event_name,
        LAG(a.real_date, :lag_n) OVER (PARTITION BY a.pacienteid ORDER BY a.real_date) AS previous_real_date,
        LAG(a.spare1, :lag_n) OVER (PARTITION BY a.pacienteid ORDER BY a.real_date) AS spare1_start,
        LAG(a.spare2, :lag_n) OVER (PARTITION BY a.pacienteid ORDER BY a.real_date) AS spare2_start,
        a.spare1 AS spare1_end,  -- spare1 para o start_event
        a.spare2 AS spare2_end,  -- spare2 para o start_event
        a.risk_class
    FROM risk_class a 
    WHERE a.rn = 1
    ORDER BY a.pacienteid, a.real_date
), delta_calculations AS (
    SELECT
        pacienteid,
        ':delta_name' AS delta_name,
        CASE 
            WHEN real_date - previous_real_date > INTERVAL '0' THEN EXTRACT(EPOCH FROM real_date - previous_real_date) / 60
            ELSE NULL
        END AS delta_minutes,
        spare1_end,
        spare2_end,
        spare1_start,
        spare2_start,
        risk_class 
    FROM lags 
    WHERE event_name = ':end_event' AND previous_event_name = ':start_event' 
)
SELECT
    delta_name,
    AVG(delta_minutes) AS avg_delta_minutes,
    STDDEV(delta_minutes) AS stddev_delta_minutes,
    MIN(delta_minutes) AS min_delta_minutes,
    MAX(delta_minutes) AS max_delta_minutes,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY delta_minutes) AS p25_delta_minutes,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delta_minutes) AS p50_delta_minutes,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY delta_minutes) AS p75_delta_minutes,
    COUNT(delta_minutes) AS count_events,
    risk_class,
    :spare1_start as spare1_start,
    :spare2_start as spare2_start,
    :spare1_end as spare1_end,
    :spare2_end as spare2_end,
    ':start_interval'::timestamp without time zone AS start_interval,
    ':end_interval'::timestamp without time zone AS end_interval
FROM delta_calculations
GROUP BY delta_name, risk_class;