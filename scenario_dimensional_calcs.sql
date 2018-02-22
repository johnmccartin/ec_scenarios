

MIN_PARCEL_AREA = 17,000
MIN_SHORT_SIDE = 90
MIN_LONG_SIDE = MIN_PARCEL_AREA / actual_short_side

select



if parcel_area >= MIN_PARCEL_AREA
	if either
		parcel_depth >= 90 ft and parcel_frontage >= 180 ft
	or
		parcel_depth >= 120 ft and parcel_frontage >= 135 ft
	or
		parcel_depth >= 160 ft and parcel_frontage >= 100
		

------------------------------------------------------


if parcel_depth >= 80 and parcel_area >= 17000 then comm_possible = true
------------------------------------------------------


alter table p1533_cambridge_scenarios.corridor_fronts
	rename column frontlen to frontstr;

alter table p1533_cambridge_scenarios.corridor_fronts
	add column frontlen int null;

update p1533_cambridge_scenarios.corridor_fronts
	set frontlen = cast(frontstr as int);


alter table p1533_cambridge_scenarios.scenario_parcels_masterlist
	rename to scenario_parcels_masterlist_tmp;

drop table if exists p1533_cambridge_scenarios.scenario_parcels_masterlist;
create table p1533_cambridge_scenarios.scenario_parcels_masterlist as
	select
		parcels.*,
		fronts.frontlen as frontage
	from p1533_cambridge_scenarios.scenario_parcels_masterlist_tmp as parcels
	left join p1533_cambridge_scenarios.corridor_fronts as fronts
	on parcels.maplot = fronts.maplot;

drop index if exists scenario_parcels_masterlist_geom_idx;
create index scenario_parcels_masterlist_geom_idx
	on p1533_cambridge_scenarios.scenario_parcels_masterlist
	using gist(geom);

alter table p1533_cambridge_scenarios.scenario_parcels_masterlist
	add column frontage_ft int null;

update p1533_cambridge_scenarios.scenario_parcels_masterlist
	set frontage_ft = frontage * 3.28084;



alter table p1533_cambridge_scenarios.scenario_parcels_masterlist
	add column depth_cutoff int null;

update p1533_cambridge_scenarios.scenario_parcels_masterlist as parcel
	set depth_cutoff =
		case
			when ST_Within(parcel.geom,ST_Buffer(fronts.geom,60)) then 60
			when ST_Within(parcel.geom,ST_Buffer(fronts.geom,80)) then 80
			when ST_Within(parcel.geom,ST_Buffer(fronts.geom,90)) then 90
			when ST_Within(parcel.geom,ST_Buffer(fronts.geom,120)) then 120
			else 999
		end
	from p1533_cambridge_scenarios.corridor_fronts as fronts
	where parcel.study_area like 'massave_%' or parcel.study_area like 'cambst_%';
