drop table if exists p1533_cambridge_scenarios.studyareas;
create table p1533_cambridge_scenarios.studyareas as
	select
		study_area,
		ST_Union(geom) as geom
	from p1533_cambridge_scenarios.scenario_parcels_masterlist
	group by study_area;

drop index if exists studyareas_geom_idx;
create index studyareas_geom_idx
	on p1533_cambridge_scenarios.studyareas
	using gist(geom);

drop table if exists p1533_cambridge_scenarios.studyareas_b200;
create table p1533_cambridge_scenarios.studyareas_b200 as
	select
		study_area,
		ST_Buffer(geom, 200) as geom
	from p1533_cambridge_scenarios.studyareas;

drop index if exists studyareas_b200_geom_idx;
create index studyareas_b200_geom_idx
	on p1533_cambridge_scenarios.studyareas_b200
	using gist(geom);



drop table if exists p1533_cambridge_scenarios.studyareas_projs;
create table p1533_cambridge_scenarios.studyareas_projs as
	select
		area.study_area,
		array_agg(devtlog.projectid) projects
	from p1533_cambridge_scenarios.studyareas_b200 as area,
		 p1533_cambridge.devt_log_hist as devtlog
	where ST_Intersects(area.geom,devtlog.geom)
	group by area.study_area;


drop table if exists p1533_cambridge_scenarios.devt_log_hist_use;
create table p1533_cambridge_scenarios.devt_log_hist_use (
	id varchar,
	resi_gfa int,
	comm_gfa int,
	retail_gfa int,
	lab_gfa int,
	insti_gfa int,
	indus_gfa int,
	other_gfa int
	);


drop table if exists p1533_cambridge_scenarios.studyarea_histdevt;
create table p1533_cambridge_scenarios.studyarea_histdevt as
	select
		area.study_area,
		sum(proj.resi_gfa) resi_gfa,
		sum(proj.comm_gfa) comm_gfa,
		sum(proj.retail_gfa) retail_gfa,
		sum(proj.lab_gfa) lab_gfa,
		sum(proj.insti_gfa) insti_gfa,
		sum(proj.indus_gfa) indus_gfa,
		sum(proj.other_gfa) other_gfa
	from p1533_cambridge_scenarios.studyareas_projs area,
		 p1533_cambridge_scenarios.devt_log_hist_use proj
	where proj.id = any (area.projects::varchar[])
	and proj.id != '281'
	group by area.study_area;

select * from p1533_cambridge_scenarios.studyarea_histdevt;





--no longer relevant


drop table if exists p1533_cambridge_scenarios.devtlog_i;
create table p1533_cambridge_scenarios.devtlog_i as
	select
		devtlog.projectid,
		array_agg(buf.study_area) study_area
from p1533_cambridge.devt_log_hist devtlog
left join p1533_cambridge_scenarios.studyareas_b200 buf
on ST_Intersects(devtlog.geom,buf.geom)
group by devtlog.projectid;

select * from p1533_cambridge_scenarios.devtlog_i;

