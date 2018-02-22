

--I
do $$

declare BASELINE__GFA_DELTA_THRESHOLD int := 10000;
declare BASELINE__GFA_RATIO_THRESHOLD numeric(5,2) := 0.5;
declare BASELINE__MIN_COMM_DEPTH int := 80;
declare BASELINE__MIN_COMM_AREA int := 17000;
declare BASELINE__RESI_INCENTIVE_THRESHOLD numeric(5,2) := 1.5;
declare PIPELINE__COMM_AS_PCT_COMMRD numeric(5,2) := 0.5;


begin

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
	set pl_comm = pl_commrd * PIPELINE__COMM_AS_PCT_COMMRD, 
		pl_lab = pl_commrd * ( 1 - PIPELINE__COMM_AS_PCT_COMMRD);


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
		master.dist_type,
		master.frontage_ft,
		master.depth_cutoff
	from p1533_cambridge_scenarios.parcels_citywide_clean_tmp2 parcels
	left join p1533_cambridge_scenarios.scenario_parcels_masterlist master
	on parcels.maplot = master.maplot;

drop index if exists parcels_citywide_clean_geom_idx;
create index parcels_citywide_clean_geom_idx
	on p1533_cambridge_scenarios.parcels_citywide_clean
	using gist(geom);

alter table p1533_cambridge_scenarios.parcels_citywide_clean
	add column comm_possible boolean default false;

update p1533_cambridge_scenarios.parcels_citywide_clean
	set comm_possible =
		case
			when landarea_new >= BASELINE__MIN_COMM_AREA and depth_cutoff >= BASELINE__MIN_COMM_DEPTH  and (study_area like 'cambst_%' or study_area like 'massave_%')
			then true
			when landarea_new >= BASELINE__MIN_COMM_AREA and (study_area is not null and study_area not like 'cambst_%' and study_area not like 'massave_%')
			then true
			else false
		end;


--clean up
drop table if exists p1533_cambridge_scenarios.parcels_citywide_clean_tmp;
drop table if exists p1533_cambridge_scenarios.parcels_citywide_clean_tmp2;
drop index if exists parcels_citywide_clean_tmp_geom_idx;



/***************************************************

	part 1: calcualte allowed gfa for each parcel

***************************************************/

drop table if exists p1533_cambridge_scenarios.scenario_parcels;
create table p1533_cambridge_scenarios.scenario_parcels as
	select
		*
	from p1533_cambridge_scenarios.parcels_citywide_clean as citywide
	where citywide.study_area is not null;

drop index if exists scenario_parcels_geom_idx;
create index scenario_parcels_geom_idx
	on p1533_cambridge_scenarios.scenario_parcels
	using gist(geom);

alter table p1533_cambridge_scenarios.scenario_parcels
	add primary key (maplot);


--Intersect parcels by base zoning. This should leave all parcel geometries there, just split into n geometries where the parcel is covered by more than one.
drop table if exists p1533_cambridge_scenarios.parcel_base_intersection;
create table p1533_cambridge_scenarios.parcel_base_intersection as
	select
		parcel.maplot,
		parcel.landarea_new,
		parcel.depth_cutoff,
		parcel.comm_possible,
		base.zone_type as zoning_base,
		ST_Intersection(parcel.geom,base.geom) as geom
	from p1533_cambridge_scenarios.scenario_parcels as parcel,
		 cambridgecurrent.cdd_zoningdistricts as base
	where ST_Intersects(parcel.geom,base.geom);

drop index if exists parcel_base_intersection_geom_idx;
create index parcel_base_intersection_geom_idx
	on p1533_cambridge_scenarios.parcel_base_intersection
	using gist(geom);


--Intersect parcel-base intersection with overlay districts. This should leave only geometries covered by overlays.
drop table if exists p1533_cambridge_scenarios.parcel_ovr_intersection;
create table p1533_cambridge_scenarios.parcel_ovr_intersection as
	select
		parbase.maplot,
		parbase.landarea_new,
		parbase.depth_cutoff,
		parbase.comm_possible,
		parbase.zoning_base,
		ovr.name as zoning_overlay,
		ST_Intersection(parbase.geom,ovr.geom) as geom
	from p1533_cambridge_scenarios.parcel_base_intersection as parbase,
		 p1533_cambridge_scenarios.zoning_overlay_edits as ovr
	where ST_Intersects(parbase.geom,ovr.geom);

drop index if exists parcel_ovr_intersection_geom_idx;
create index parcel_ovr_intersection_geom_idx
	on p1533_cambridge_scenarios.parcel_ovr_intersection
	using gist(geom);




drop table if exists p1533_cambridge_scenarios.zoning_overlay_edits_union;
create table p1533_cambridge_scenarios.zoning_overlay_edits_union as
	select
		ST_Union(geom) as geom
	from p1533_cambridge_scenarios.zoning_overlay_edits;

drop index if exists zoning_overlay_edits_union_geom_idx;
create index zoning_overlay_edits_union_geom_idx
	on p1533_cambridge_scenarios.zoning_overlay_edits_union
	using gist(geom);

drop table if exists p1533_cambridge_scenarios.parcel_ovr_difference2;

--Difference parcel-base intersection with overlay districts. This should leave only geometries NOT covered by overlays.
drop table if exists p1533_cambridge_scenarios.parcel_ovr_difference;
create table p1533_cambridge_scenarios.parcel_ovr_difference as
	select
		parbase.maplot,
		parbase.landarea_new,
		parbase.depth_cutoff,
		parbase.comm_possible,
		parbase.zoning_base,
		cast(null as varchar) as zoning_overlay,
		parbase.geom
	from p1533_cambridge_scenarios.parcel_base_intersection as parbase,
		 p1533_cambridge_scenarios.zoning_overlay_edits_union as ovr
	where ST_Disjoint(parbase.geom,ST_Buffer(ovr.geom,-0.1));

drop index if exists parcel_ovr_difference_geom_idx;
create index parcel_ovr_difference_geom_idx
	on p1533_cambridge_scenarios.parcel_ovr_difference
	using gist(geom);


--Merge the two layers modified by overlay geoms
drop table if exists p1533_cambridge_scenarios.parcel_zng_merge;
create table p1533_cambridge_scenarios.parcel_zng_merge as
	select * from p1533_cambridge_scenarios.parcel_ovr_intersection
	union
	select * from p1533_cambridge_scenarios.parcel_ovr_difference;

drop index if exists parcel_zng_merge_geom_idx;
create index parcel_zng_merge_geom_idx
	on p1533_cambridge_scenarios.parcel_zng_merge
	using gist(geom);


-- Calculate the land area of the subparcel and create the target field to join in the FAR info
alter table p1533_cambridge_scenarios.parcel_zng_merge
	add column part_land int,
	add column zoning_condition varchar null;

update p1533_cambridge_scenarios.parcel_zng_merge
	set part_land = ST_Area(geom);

update p1533_cambridge_scenarios.parcel_zng_merge
	set zoning_condition = zoning_base
	where zoning_overlay is null;

update p1533_cambridge_scenarios.parcel_zng_merge
	set zoning_condition = zoning_base || ' + ' || zoning_overlay
	where zoning_overlay is not null;




-- Join the FAR info
drop table if exists p1533_cambridge_scenarios.parcel_far_join;
create table p1533_cambridge_scenarios.parcel_far_join as
	select
		parcel.*,
		far.far_comm,
		far.far_resi,
		far.far_resinc,
		far.far_max,
		far.far_ratio
	from p1533_cambridge_scenarios.parcel_zng_merge as parcel
	left join p1533_cambridge_scenarios.zoning_lookup as far
	on parcel.zoning_condition = far.zng_condition;

alter table p1533_cambridge_scenarios.parcel_far_join
	add column allowed_gfa_resi int default null,
	add column allowed_gfa_comm int default null;

update p1533_cambridge_scenarios.parcel_far_join
	set allowed_gfa_resi = part_land * far_resinc,
		allowed_gfa_comm = part_land * far_comm;


-- Sum the allowed GFA from subparcels into the parcel itself
-- THE MAPLOT WILL NEED TO SERVE AS PRIMARY KEY
drop table if exists p1533_cambridge_scenarios.parcel_new_development;
create table p1533_cambridge_scenarios.parcel_new_development as
	select
		parcel.*,
		array_agg(subparcel.zoning_condition) as zoning_condition,
		sum(subparcel.allowed_gfa_resi) as allowed_gfa_resi,
		sum(subparcel.allowed_gfa_comm) as allowed_gfa_comm
	from p1533_cambridge_scenarios.scenario_parcels as parcel
	left join p1533_cambridge_scenarios.parcel_far_join as subparcel
	on parcel.maplot = subparcel.maplot
	group by parcel.maplot; -- THIS NEEDS TO BE THE PRIMARY KEY

drop index if exists parcel_new_development_geom_idx;
create index parcel_new_development_geom_idx
	on p1533_cambridge_scenarios.parcel_new_development
	using gist(geom);

alter table p1533_cambridge_scenarios.parcel_new_development
	add column allowed_gfa_choice int default null,
	add column scenario_use varchar default null;

update p1533_cambridge_scenarios.parcel_new_development
	set allowed_gfa_choice =
		case
			when comm_possible = true and (allowed_gfa_comm = 0 or allowed_gfa_comm is null) then allowed_gfa_resi
			when comm_possible = true and ( allowed_gfa_resi / allowed_gfa_comm >= BASELINE__RESI_INCENTIVE_THRESHOLD ) then allowed_gfa_resi
			when comm_possible = true and ( allowed_gfa_resi / allowed_gfa_comm < BASELINE__RESI_INCENTIVE_THRESHOLD ) then allowed_gfa_comm
			when comm_possible = false then allowed_gfa_resi
		end;

update p1533_cambridge_scenarios.parcel_new_development
	set scenario_use =
		case
			when comm_possible = true and (allowed_gfa_comm = 0 or allowed_gfa_comm is null) then 'resi'
			when comm_possible = true and ( allowed_gfa_resi / allowed_gfa_comm >= BASELINE__RESI_INCENTIVE_THRESHOLD ) then 'resi'
			when comm_possible = true and ( allowed_gfa_resi / allowed_gfa_comm < BASELINE__RESI_INCENTIVE_THRESHOLD ) then 'nonresi'
			when comm_possible = false then 'resi'
		end;


-- Calculate GFA Delta and GFA Ratio
alter table p1533_cambridge_scenarios.parcel_new_development
	add column gfa_delta real null,
	add column gfa_ratio numeric(5,2) null;

update p1533_cambridge_scenarios.parcel_new_development
	set gfa_delta = 
		case
			when built_gfa is not null
			then allowed_gfa_choice - built_gfa
			else allowed_gfa_choice
		end;

update p1533_cambridge_scenarios.parcel_new_development
	set gfa_ratio = 
		case 
			when built_gfa > 0 and built_gfa is not null
			then gfa_delta / built_gfa
			else 999
		end; 


alter table p1533_cambridge_scenarios.parcel_new_development
	add column allowed_gfa_resi_guaranteed int default 0,
	add column allowed_gfa_resi_contingent int default 0,
	add column allowed_gfa_comm_contingent int default 0;

update p1533_cambridge_scenarios.parcel_new_development
	set allowed_gfa_resi_guaranteed = allowed_gfa_resi 
	where scenario_use = 'resi';

update p1533_cambridge_scenarios.parcel_new_development
	set	allowed_gfa_resi_contingent = allowed_gfa_resi 
	where scenario_use = 'nonresi';

update p1533_cambridge_scenarios.parcel_new_development
	set	allowed_gfa_comm_contingent = allowed_gfa_comm 
	where scenario_use = 'nonresi';


alter table p1533_cambridge_scenarios.parcel_new_development
	add column landarea_guaranteed int default 0,
	add column landarea_contingent int default 0,
	add column built_resi_guaranteed int default 0,
	add column built_resi_contingent int default 0,
	add column built_nonresi_guaranteed int default 0,
	add column built_nonresi_contingent int default 0,
	add column built_units_guaranteed int default 0,
	add column built_units_contingent int default 0,
	add column built_retail_guaranteed int default 0,
	add column built_retail_contingent int default 0;

update p1533_cambridge_scenarios.parcel_new_development
	set	landarea_guaranteed = landarea_new 
	where scenario_use = 'resi';

update p1533_cambridge_scenarios.parcel_new_development
	set	landarea_contingent = landarea_new 
	where scenario_use = 'nonresi';

update p1533_cambridge_scenarios.parcel_new_development
	set	built_resi_guaranteed = built_resi 
	where scenario_use = 'resi';

update p1533_cambridge_scenarios.parcel_new_development
	set	built_resi_contingent = built_resi 
	where scenario_use = 'nonresi';


update p1533_cambridge_scenarios.parcel_new_development
	set	built_nonresi_guaranteed = built_gfa - built_resi 
	where scenario_use = 'resi';

update p1533_cambridge_scenarios.parcel_new_development
	set	built_nonresi_contingent = built_gfa - built_resi 
	where scenario_use = 'nonresi';

update p1533_cambridge_scenarios.parcel_new_development
	set	built_units_guaranteed = built_units
	where scenario_use = 'resi';

update p1533_cambridge_scenarios.parcel_new_development
	set	built_units_contingent = built_units 
	where scenario_use = 'nonresi';


update p1533_cambridge_scenarios.parcel_new_development
	set	built_retail_guaranteed = built_retail
	where scenario_use = 'resi';

update p1533_cambridge_scenarios.parcel_new_development
	set	built_retail_contingent = built_retail 
	where scenario_use = 'nonresi';



/***************************************************

	part 2: aggregate parcels

***************************************************/

/*
drop table if exists p1533_cambridge_scenarios.parcel_aggregate_!;
create table p1533_cambridge_scenarios.parcel_aggregate_1 as
	select
		newdevt.*,
		get_neighbors()
	from p1533_cambridge_scenarios.parcel_new_development newdevt;

drop index if exists parcel_aggregate_1_geom_idx;
create index parcel_aggregate_1_geom_idx
	on p1533_cambridge_scenarios.parcel_aggregate_1
	using gist(geom)

*/





/***************************************************

	part 3: choose softsites

***************************************************/


alter table p1533_cambridge_scenarios.parcel_new_development
	add column softsite boolean default false;

update p1533_cambridge_scenarios.parcel_new_development
	set softsite =
		case
			when 
				(
					gfa_delta >= BASELINE__GFA_DELTA_THRESHOLD
				and gfa_ratio >= BASELINE__GFA_RATIO_THRESHOLD
				)
				and (never_soft = 0 or never_soft is null)
			then true

			else false
		end;







/***************************************************

	part 4: aggregate figures by study area

***************************************************/

/*

drop table if exists p1533_cambridge_scenarios.baseline_calcs_by_area;
create table p1533_cambridge_scenarios.baseline_calcs_by_area as
	select
		dist_type,
		study_area,
		sum(tot_gfa) exis_gfa,
		sum(resi_gfa) exis_resi,
		sum(h_units) exis_units,
		sum(comm_gfa) exis_comm,
		sum(retail_gfa) exis_retail,
		sum(lab_gfa) exis_lab,
		sum(insti_gfa) exis_insti,
		sum(indus_gfa) exis_indus,
		sum(other_gfa) exis_other,

		sum(pl_gfa) pl_gfa,
		sum(pl_resi) pl_resi,
		sum(pl_units) pl_units,
		sum(pl_comm) pl_comm,
		sum(pl_retail) pl_retail,
		sum(pl_lab) pl_lab,
		sum(pl_insti) pl_insti,
		sum(pl_other) pl_other,

		sum(built_gfa) built_gfa,
		sum(built_resi) built_resi,
		sum(built_units) built_units,
		sum(built_comm) built_comm,
		sum(built_retail) built_retail,
		sum(built_lab) built_lab,
		sum(built_insti) built_insti,
		sum(built_indus) built_indus,
		sum(built_other) built_other,

		sum(allowed_gfa_choice) allowed_gfa_choice,
		sum(allowed_gfa_resi_guaranteed) allowed_gfa_resi_guaranteed,
		sum(allowed_gfa_resi_contingent) allowed_gfa_resi_contingent,
		sum(allowed_gfa_comm_contingent) allowed_gfa_comm_contingent

	from p1533_cambridge_scenarios.parcel_new_development
	where softsite = true
	group by dist_type, study_area
	order by dist_type, study_area;

*/


drop table if exists p1533_cambridge_scenarios.baseline_calcs_by_area;
create table p1533_cambridge_scenarios.baseline_calcs_by_area as
	select
		dist_type,
		study_area,
		sum(landarea_guaranteed) landarea_guaranteed,
		sum(landarea_contingent) landarea_contingent,
		sum(built_gfa) built_gfa,
		sum(built_resi_guaranteed) built_resi_guaranteed,
		sum(built_nonresi_guaranteed) built_nonresi_guaranteed,
		sum(built_resi_contingent) built_resi_contingent,
		sum(built_nonresi_contingent) built_nonresi_contingent,
		sum(built_units_guaranteed) built_units_guaranteed,
		sum(built_units_contingent) built_units_contingent,

		sum(allowed_gfa_choice) allowed_gfa_choice,
		sum(allowed_gfa_resi_guaranteed) allowed_gfa_resi_guaranteed,
		sum(allowed_gfa_resi_contingent) allowed_gfa_resi_contingent,
		sum(allowed_gfa_comm_contingent) allowed_gfa_comm_contingent

	from p1533_cambridge_scenarios.parcel_new_development
	where softsite = true
	group by dist_type, study_area
	order by dist_type, study_area;




--clean up

drop table if exists
	p1533_cambridge_scenarios.parcel_base_intersection,
	p1533_cambridge_scenarios.parcel_far_join,
	p1533_cambridge_scenarios.parcel_ovr_difference,
	p1533_cambridge_scenarios.parcel_ovr_intersection,
	p1533_cambridge_scenarios.parcel_zng_merge,
	p1533_cambridge_scenarios.parcels_citywide_clean,
	p1533_cambridge_scenarios.scenario_parcels,
	p1533_cambridge_scenarios.zoning_overlay_edits_union;




end 
$$ language plpgsql;


select
	distinct study_area
from p1533_cambridge_scenarios.parcel_new_development;





select
	*
from p1533_cambridge_scenarios.baseline_calcs_by_area;