
create table public.events_day_backup as
select
*
from public.events_day

drop table public.events_day;


CREATE TABLE IF NOT EXISTS public.events_day
(
    delta_name text COLLATE pg_catalog."default",
    avg_delta_minutes double precision,
    stddev_delta_minutes double precision,
    min_delta_minutes double precision,
    max_delta_minutes double precision,
    p25_delta_minutes double precision,
    p50_delta_minutes double precision,
    p75_delta_minutes double precision,
    count_events integer,
    risk_class text COLLATE pg_catalog."default",
    spare1_start_count_json jsonb,
    spare2_start_count_json jsonb,
    spare1_end_count_json jsonb,
    spare2_end_count_json jsonb,
    start_interval timestamp without time zone NOT NULL,
    end_interval timestamp without time zone,
    CONSTRAINT unique_delta_name_interval_risk_class_day UNIQUE (delta_name, start_interval, end_interval, risk_class)
) 

insert into public.events_day
select
*
from public.events_day_backup;

drop table public.events_day_backup;

