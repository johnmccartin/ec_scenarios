/***************************************************

	part 1: pull from the master parcel table (buildout-assessing)
			and prep for use in scenarios

***************************************************/

drop table if exists p1533_cambridge_scenarios.parcels_citywide_clean_tmp;
create table p1533_cambridge_scenarios.parcels_citywide_clean_tmp as
	select
		geom,
		maplot,
		ST_Area(geom) as landarea_new,
		lu_scen,
		far,
		yr_blt,
		case
			when tot_gfa = 0 and (res_gfa_asr > 0 or units > 0 or res_gfa_bo > 0 or comm_gfa > 0 or indus_gfa > 0 or insti_gfa > 0 or trnsp_gfa > 0 or prkg_gfa > 0)
			then 
				case
					when (res_gfa_bo + nonres_gfa_bo > res_gfa_asr + comm_gfa + indus_gfa + insti_gfa + trnsp_gfa + prkg_gfa)
					then res_gfa_bo + nonres_gfa_bo
					else res_gfa_asr + comm_gfa + indus_gfa + insti_gfa + trnsp_gfa + prkg_gfa
				end
			else tot_gfa
		end as tot_gfa,
		units as h_units,
		res_gfa_bo as resi_gfa,
		nonres_gfa_bo,
		case
			when lu_scen = 'commercial'
			then nonres_gfa_bo
			else 0
		end as comm_gfa,
		case
			when lu_scen = 'retail' or lu_scen = 'mixed_use'
			then nonres_gfa_bo
			else 0
		end as retail_gfa,
		case
			when lu_scen = 'R&D'
			then nonres_gfa_bo
			else 0
		end as lab_gfa,
		case
			when lu_scen = 'institutional'
			then nonres_gfa_bo
			when lu_scen = 'mixed use'
			then
				case
					when usecode = '0942'
					then insti_gfa
					else 0
				end
			else 0
		end as insti_gfa,
		case
			when lu_scen = 'industrial'
			then nonres_gfa_bo
			else 0
		end as indus_gfa,
		case
			when lu_scen in ('open space','transportation','other')
			then nonres_gfa_bo
			else 0
		end as other_gfa,
		proj_id as pl_id,
		pl_gfa,
		pl_resunits as pl_units,
		pl_affunits
		--this is all we can pull from the buildout assesing file
		--but there's more pl data in the pl use table
	from p1533_cambridge.buildout_assessing;

drop index if exists parcels_citywide_clean_tmp_geom_idx;
create index parcels_citywide_clean_tmp_geom_idx
	on p1533_cambridge_scenarios.parcels_citywide_clean_tmp
	using gist(geom);



/***************************************************

	part 2: pull additional pipeline data to calculate
			net new GFAs later

THIS ONLY HAPPENS ONCE


--create table for csv import
drop table if exists p1533_cambridge.devt_log_use_current_013018;
create table p1533_cambridge.devt_log_use_current_013018 (
	proj_id varchar default null primary key,
	pl_resi int default 0,
	pl_commrd int default 0,
	pl_retail int default 0,
	pl_insti int default 0,
	pl_other int default 0
);

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		YOU MUST IMPORT THE PIPELINE USE CSV NOW

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~	 


***************************************************					*/


delete from p1533_cambridge_scenarios.parcels_citywide_clean_tmp
	where maplot in ( '171-1','267.3-275','104-96','130-144','14-31','1A-102','1A-30','267.2-262','30-37');

drop index if exists parcels_citywide_clean_tmp_ml_idx;
create index parcels_citywide_clean_tmp_ml_idx
	on p1533_cambridge_scenarios.parcels_citywide_clean_tmp
	using btree(maplot);



alter table p1533_cambridge_scenarios.parcels_citywide_clean_tmp
	add primary key (maplot);


drop table if exists p1533_cambridge_scenarios.parcels_citywide_clean_tmp2;
create table p1533_cambridge_scenarios.parcels_citywide_clean_tmp2 as
	select 
		parcels.*,
		sum(pl_resi) pl_resi,
		sum(pl_commrd) pl_commrd,
		sum(pl_retail) pl_retail,
		sum(pl_insti) pl_insti,
		sum(pl_other) pl_other
	from p1533_cambridge_scenarios.parcels_citywide_clean_tmp parcels
	left join p1533_cambridge.devt_log_use_current_013018 pl
	on pl.proj_id = any (parcels.pl_id::varchar[])
	group by parcels.maplot;


alter table p1533_cambridge_scenarios.parcels_citywide_clean_tmp2
	add column pl_comm int default 0,
	add column pl_lab int default 0;

update p1533_cambridge_scenarios.parcels_citywide_clean_tmp2
	set pl_comm = pl_commrd * 0.5, --comm and r&d assumed to be evenly split
		pl_lab = pl_commrd * 0.5; --comm and r&d assumed to be evenly split


/***************************************************

	part 3: select existing and pipeline columns to get "as-built" condition 

***************************************************/

alter table p1533_cambridge_scenarios.parcels_citywide_clean_tmp2
	add column built_gfa int default 0,
	add column built_resi int default 0,
	add column built_units int default 0,
	add column built_comm int default 0,
	add column built_retail int default 0,
	add column built_lab int default 0,
	add column built_insti int default 0,
	add column built_indus int default 0,
	add column built_other int default 0;

update p1533_cambridge_scenarios.parcels_citywide_clean_tmp2
	set
		built_gfa = case when pl_gfa is not null and pl_gfa > 0 then pl_gfa else tot_gfa end,
		built_resi = case when pl_resi is not null and pl_resi > 0 then pl_resi else resi_gfa end,
		built_units = case when pl_units is not null and pl_units > 0 then pl_units else h_units end,
		built_comm = case when pl_comm is not null and pl_comm > 0 then pl_comm else comm_gfa end,
		built_retail = case when pl_retail is not null and pl_retail > 0 then pl_retail else retail_gfa end,
		built_lab = case when pl_lab is not null and pl_lab > 0 then pl_lab else lab_gfa end,
		built_insti = case when pl_insti is not null and pl_insti > 0 then pl_insti else insti_gfa end,
		built_indus = indus_gfa,
		built_other = case when pl_other is not null and pl_other > 0 then pl_other else other_gfa end;





/***************************************************

	part 4: gather info from scenario parcel master list

***************************************************/

drop table if exists p1533_cambridge_scenarios.parcels_citywide_clean;
create table p1533_cambridge_scenarios.parcels_citywide_clean as
	select
		parcels.*,
		master.study_area,
		master.neversoft as never_soft,
		master.dist_type
	from p1533_cambridge_scenarios.parcels_citywide_clean_tmp2 parcels
	left join p1533_cambridge_scenarios.scenario_parcels_masterlist master
	on parcels.maplot = master.maplot;

drop index if exists parcels_citywide_clean_geom_idx;
create index parcels_citywide_clean_geom_idx
	on p1533_cambridge_scenarios.parcels_citywide_clean
	using gist(geom);


--clean up
drop table if exists p1533_cambridge_scenarios.parcels_citywide_clean_tmp;
drop table if exists p1533_cambridge_scenarios.parcels_citywide_clean_tmp2;
drop index if exists parcels_citywide_clean_tmp_geom_idx;


