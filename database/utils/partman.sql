
SELECT partman.create_parent(
    p_parent_table    := 'source.acolhimento'::text,
    p_control         := 'datahora'::text,
    p_interval        := '1 day'::text,
    p_start_partition := (now() - interval '10 days')::date::text,
    p_premake         := 3
);


UPDATE partman.part_config
SET retention = '90 days', 
	retention_keep_table = false,
    retention_keep_index = false
WHERE parent_table = 'source.acolhimento';


delete from partman.part_config
WHERE parent_table = 'source.acolhimento';