update p1533_cambridge_scenarios.scenario_parcels_masterlist
	set neversoft = 1
	where maplot in ('83-48');

update p1533_cambridge_scenarios.scenario_parcels_masterlist
	set neversoft = 0
	where maplot in ('27-25','55-25','66-124','108-84','178-21','153-80','153-81','153-85','173-67','184-106','200-90','152-7','106-105','95-58','87-37','16-6','10-16','80-128','179-51','153-82','176-53','1A-82','134-50');
	or maplot is null;