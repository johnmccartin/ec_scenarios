update p1533_cambridge_scenarios.scenario_parcels_masterlist
	set neversoft = 1
	where maplot in ('91-199','91-190','91-111','91-108','35-100','35-102','22-130','35-101','22-132','35-95','22-133');

update p1533_cambridge_scenarios.scenario_parcels_masterlist
	set neversoft = 0
	where maplot in ('35-88','35-87')
	or maplot is null;