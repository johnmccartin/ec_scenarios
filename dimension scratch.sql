alter table p1533_cambridge_scenarios.parcel_far_join
	add column scenario_use varchar null;

update p1533_cambridge_scenarios.parcel_far_join
	set scenario_use =
		case
			when comm_possible = true and far_ratio < 1.5
			then 'comm_etc'
			else 'resi'
		end;


alter table p1533_cambridge_scenarios.parcel_far_join
	add column allowed_gfa int null;


update p1533_cambridge_scenarios.parcel_far_join
	set allowed_gfa = 
		case
			when scenario_use = 'comm_etc'
			then part_land * far_comm
			when scenario_use = 'resi'
			then part_land * far_resinc
		end;


alter table p1533_cambridge_scenarios.parcel_new_development
	add column allowed_nonresi int default 0,
	add column allowed_resi int default 0;

update p1533_cambridge_scenarios.parcel_new_development
	set allowed_nonresi = allowed_gfa
	where not 'resi' = any (scenario_use);

update p1533_cambridge_scenarios.parcel_new_development
	set allowed_resi = allowed_gfa
	where 'resi' = any (scenario_use);