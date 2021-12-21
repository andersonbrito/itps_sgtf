rule arguments:
	params:
		datadir = "data",
		rename_file = "data/rename_columns.xlsx",
		correction_file = "data/rename_values.xlsx",
		shapefile = "/Users/anderson/GLab Dropbox/Anderson Brito/codes/geoCodes/bra_adm_ibge_2020_shp/bra_admbnda_adm2_ibge_2020.shp",
		index_column = "division_exposure",
		date_column = "date_testing",
		start_date = "2021-11-01",
		end_date = "2021-12-31"


arguments = rules.arguments.params


rule reshape:
	message:
		"""
		Combine tables with testing data
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file
	params:
		datadir = arguments.datadir
	output:
		matrix = "results/combined_testdata.tsv"
	shell:
		"""
		python3 scripts/reshape_testdata.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--output {output.matrix}
		"""


rule geomatch:
	message:
		"""
		Match location names with geographic shapefile polygons
		"""
	input:
		input_file =  "results/combined_testdata.tsv",
		shapefile = arguments.shapefile
	params:
		geo_columns = "state, location",
		add_geo = "country:Brazil",
		lat = "lat",
		long = "long",
		check_match = "ADM2_PT",
		target = "ADM1_PT, ADM1_PCODE, ADM2_PT, ADM2_PCODE"
	output:
		matrix = "results/combined_testdata_geo.tsv"
	shell:
		"""
		python3 scripts/name2shape.py \
			--input {input.input_file} \
			--shapefile \"{input.shapefile}\" \
			--geo-columns \"{params.geo_columns}\" \
			--add-geo {params.add_geo} \
			--lat {params.lat} \
			--long {params.long} \
			--check-match {params.check_match} \
			--target \"{params.target}\" \
			--output {output.matrix}
		"""


rule sgtf_states:
	message:
		"""
		Aggregate data related to SGTF results by state
		"""
	input:
		input_file = "results/combined_testdata_geo.tsv"
	params:
		xvar = arguments.date_column,
		xtype = "time",
		format = "integer",
		yvar = "ADM2_PCODE S_detection",
		index = "ADM2_PCODE",
		extra_columns = "ADM1_PT ADM2_PT",
		filters = "~test_result:Negative",
		start_date = arguments.start_date,
		end_date = arguments.end_date
	output:
		matrix = "results/matrix_states_sgtf_detection.tsv"
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar} \
			--unique-id {params.index} \
			--extra-columns  {params.extra_columns} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix}
		"""


rule alltests_states:
	message:
		"""
		Aggregate counts of all SGTF tests by state
		"""
	input:
		input_file = "results/combined_testdata_geo.tsv"
	params:
		xvar = arguments.date_column,
		xtype = "time",
		format = "integer",
		yvar = "ADM2_PCODE",
		index = "ADM2_PCODE",
		extra_columns = "location ADM1_PT ADM2_PT",
		filters = "~test_result:Negative",
		start_date = arguments.start_date,
		end_date = arguments.end_date
	output:
		matrix = "results/matrix_states_sgtf_denominators.tsv"
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar} \
			--unique-id {params.index} \
			--extra-columns  {params.extra_columns} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix}
		"""


rule percent_states:
	message:
		"""
		Percentage of STGF cases per state
		"""
	input:
		input1 = "results/matrix_states_sgtf_detection.tsv",
		input2 = "results/matrix_states_sgtf_denominators.tsv"
	params:
		index1 = "ADM2_PCODE S_detection",
		index2 = "ADM2_PCODE"
	output:
		matrix = "results/matrix_states_sgtf_percentages.tsv"
	shell:
		"""
		python3 scripts/normdata.py \
			--input1 {input.input1} \
			--input2 {input.input2} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--output {output.matrix}
		"""


#rule xxx:
#	message:
#		"""
#		
#		"""
#	input:
#		metadata = arguments.
#	params:
#		index = arguments.,
#		date = arguments.
#	output:
#		matrix = "results/"
#	shell:
#		"""
#		python3 scripts/ \
#			--metadata {input.} \
#			--index-column {params.} \
#			--extra-columns {params.} \
#			--date-column {params.} \
#			--output {output.}
#		"""
#
#
#rule xxx:
#	message:
#		"""
#		
#		"""
#	input:
#		metadata = arguments.
#	params:
#		index = arguments.,
#		date = arguments.
#	output:
#		matrix = "results/"
#	shell:
#		"""
#		python3 scripts/ \
#			--metadata {input.} \
#			--index-column {params.} \
#			--extra-columns {params.} \
#			--date-column {params.} \
#			--output {output.}
#		"""
#
#
#
#rule xxx:
#	message:
#		"""
#		
#		"""
#	input:
#		metadata = arguments.
#	params:
#		index = arguments.,
#		date = arguments.
#	output:
#		matrix = "results/"
#	shell:
#		"""
#		python3 scripts/ \
#			--metadata {input.} \
#			--index-column {params.} \
#			--extra-columns {params.} \
#			--date-column {params.} \
#			--output {output.}
#		"""


rule clean:
	message: "Removing directories: {params}"
	params:
		"results"
	shell:
		"""
		rm -rfv {params}
		"""
