drop table if exists p1533_cambridge_scenarios.usecode_lookup;
create table p1533_cambridge_scenarios.usecode_lookup (
	usecode varchar,
	lu_scen varchar
)

--import csv

alter table p1533_cambridge.buildout_assessing
	rename to buildout_assessing_tmp;

drop table if exists p1533_cambridge.buildout_assessing;
create table p1533_cambridge.buildout_assessing as 
	select
		bo.*,
		lookup.lu_scen
	from p1533_cambridge.buildout_assessing_tmp bo
	left join p1533_cambridge_scenarios.usecode_lookup lookup
	on bo.usecode = lookup.usecode;

drop index if exists buildout_assessing_geom_idx;
create index buildout_assessing_geom_idx
	on p1533_cambridge.buildout_assessing
	using gist(geom);

drop table if exists p1533_cambridge.buildout_assessing_tmp;