

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



/*
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

*/

drop table if exists p1533_cambridge_scenarios.corridor_fronts_b60;
create table p1533_cambridge_scenarios.corridor_fronts_b60 as
	select
		ST_Union(St_Buffer(fronts.geom,60)) as geom
	from p1533_cambridge_scenarios.corridor_fronts as fronts;
drop index if exists corridor_fronts_b60_geom_idx;
create index corridor_fronts_b60_geom_idx
	on p1533_cambridge_scenarios.corridor_fronts_b60
	using gist(geom);

drop table if exists p1533_cambridge_scenarios.corridor_fronts_b80;
create table p1533_cambridge_scenarios.corridor_fronts_b80 as
	select
		ST_Union(St_Buffer(fronts.geom,80)) as geom
	from p1533_cambridge_scenarios.corridor_fronts as fronts;
drop index if exists corridor_fronts_b80_geom_idx;
create index corridor_fronts_b80_geom_idx
	on p1533_cambridge_scenarios.corridor_fronts_b80
	using gist(geom);

drop table if exists p1533_cambridge_scenarios.corridor_fronts_b90;
create table p1533_cambridge_scenarios.corridor_fronts_b90 as
	select
		ST_Union(St_Buffer(fronts.geom,90)) as geom
	from p1533_cambridge_scenarios.corridor_fronts as fronts;
drop index if exists corridor_fronts_b90_geom_idx;
create index corridor_fronts_b90_geom_idx
	on p1533_cambridge_scenarios.corridor_fronts_b90
	using gist(geom);

drop table if exists p1533_cambridge_scenarios.corridor_fronts_b120;
create table p1533_cambridge_scenarios.corridor_fronts_b120 as
	select
		ST_Union(St_Buffer(fronts.geom,120)) as geom
	from p1533_cambridge_scenarios.corridor_fronts as fronts;
drop index if exists corridor_fronts_b120_geom_idx;
create index corridor_fronts_b120_geom_idx
	on p1533_cambridge_scenarios.corridor_fronts_b120
	using gist(geom);






alter table p1533_cambridge_scenarios.scenario_parcels_masterlist
	add column depth_cutoff2 int null;


update p1533_cambridge_scenarios.scenario_parcels_masterlist as parcel
	set depth_cutoff2 =
		case
			when ST_Within(parcel.geom,b60.geom) then 60
			when ST_Within(parcel.geom,b80.geom) then 80
			when ST_Within(parcel.geom,b90.geom) then 90
			when ST_Within(parcel.geom,b120.geom) then 120
			else 999
		end
	from p1533_cambridge_scenarios.corridor_fronts_b60 as b60,
		 p1533_cambridge_scenarios.corridor_fronts_b80 as b80,
		 p1533_cambridge_scenarios.corridor_fronts_b90 as b90,
		 p1533_cambridge_scenarios.corridor_fronts_b120 as b120
	where parcel.study_area like 'massave_%' or parcel.study_area like 'cambst_%';


drop table if exists p1533_cambridge_scenarios.corridor_fronts