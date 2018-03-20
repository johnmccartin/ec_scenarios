drop table if exists p1533_cambridge.bldg_parcel_i;
create table p1533_cambridge.bldg_parcel_i as
	select
		ST_Intersects(bldg.geom,parcel.geom) as geom
	from cambridgecurrent.basemap_buildings as bldg,
		 p1533_cambridge.buildout_assessing as parcel;
		 
drop index if exists bldg_parcel_i_geom_idx;
create index bldg_parcel_i_geom_idx
	on p1533_cambridge.bldg_parcel_i
	using gist(geom);


alter table p1533_cambridge.bldg_parcel_i
	add column built_ftpt int default null;

update p1533_cambridge.bldg_parcel_i
	set built_ftpt = ST_Area(geom);



alter table p1533_cambridge.buildout_assessing
	rename to p1533_cambridge.buildout_assessing_backup;


drop table if exists p1533_cambridge.buildout_assessing;
create table p1533_cambridge.buildout_assessing as
	select
		parcels.*,
		sum(bldg.built_ftpt) as built_ftpt
	from p1533_cambridge.buildout_assessing_backup as parcels
	left join p1533_cambridge.bldg_parcel_i as bldg
	on parcels.maplot = bldg.maplot;

 