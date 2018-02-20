
drop table if exists p1533_cambridge_scenarios.zoning_lookup;
create table p1533_cambridge_scenarios.zoning_lookup (
	zng_condition varchar default null,
	far_comm real default 0,
	far_resi real default 0,
	far_resinc real default 0,
	far_max real default 0,
	far_ratio real default 0
)