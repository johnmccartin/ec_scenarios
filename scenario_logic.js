todo
-----------------------
fix frontages for inman east


studyarea_density_lookup
-----------------------
	study_area: lookup name for each area,
	conditionA: UF condition under A scenario,
	conditionB: UF condition under B scenario,
	conditionC: UF condition under C scenario,
	factorA: footprint multiplier under A scenario,
	factorB: footprint multiplier under B scenario,
	factorC: footprint multiplier under C scneario



join studyarea_density_lookup
onto parcels (in study areas)
on study_area (name)

-----------------------------------------------------------

/* scenario A */

in parcels_studyareas :

	let bar_depth = 60;
	let retail_depth = 45;
	let upperfloor_setback_factor = .08;
	

	if depth_thresh <= 80 : // if depth is between 60 and 80 ft.

		let footprint = frontage * bar_depth;
		let allowed_gfa = footprint * factorA;
		let retail_gfa = frontage * retail_depth;
		let resi_gfa = allowed_gfa - retail_gfa;


	if depth_thresh >= 90 : // if the depth is greater than or equal to 80 ft.

		if frontage >= 120 :

			let bldg_depth = 80

			if depth_thresh >= 120 :
				bldg_depth = 110
			else if depth_thresh >= 110
				bldg_depth = 100
			else if depth_thresh = 100
				bldg_depth = 90

			let footprint = frontage * bldg_depth;
			let allowed_gfa = footprint * factorA;
			let retail_gfa = frontage * retail_depth;
			let comm_gfa = allowed_gfa - retail_gfa;

		else if frontage < 120 :
			let footprint = frontage * 120;
			let allowed_gfa = footprint * factorA;
			let retail_gfa = frontage * retail_depth;
			let comm_gfa = allowed_gfa - retail_gfa;












