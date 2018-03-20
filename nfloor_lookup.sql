drop table if exists p1533_cambridge_scenarios.corridor_floors_lookup;
create table p1533_cambridge_scenarios.corridor_floors_lookup (
	study_area varchar default null,
	floors_n int default null
);


drop table p1533_cambridge_scenarios.scenario_parcels_masterlist_tmp;
alter table p1533_cambridge_scenarios.scenario_parcels_masterlist
	rename to scenario_parcels_masterlist_tmp;


drop table if exists p1533_cambridge_scenarios.scenario_parcels_masterlist;
create table p1533_cambridge_scenarios.scenario_parcels_masterlist as 
	select
		parcels.*,
		lookup.floors_n as s1_coor_floors
	from p1533_cambridge_scenarios.scenario_parcels_masterlist_tmp as parcels
	left join p1533_cambridge_scenarios.corridor_floors_lookup as lookup
	on parcels.study_area = lookup.study_area;

drop table p1533_cambridge_scenarios.scenario_parcels_masterlist_tmp;
