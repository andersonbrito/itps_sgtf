rule all:
	input:
		matrix_c = "results/matrix_country_positives.tsv",
		matrix_s = "results/matrix_states_positives.tsv",
		matrix_l = "results/matrix_location_positives.tsv",
		sgtf_c1 = "results/matrix_country_sgtf_growth.tsv",
		sgtf_c2 = "results/matrix_country_sgtf_growth_week.tsv",
		sgtf_s1 = "results/matrix_states_sgtf_growth.tsv",
		sgtf_s2 = "results/matrix_states_sgtf_growth_week.tsv",
		posrate_c1 = "results/matrix_country_positivity.tsv",
		posrate_s1 = "results/matrix_states_positivity.tsv",


rule arguments:
	params:
		datadir = "data",
		rename_file = "data/rename_columns.xlsx",
		correction_file = "data/fix_values.xlsx",
		shapefile = "/Users/anderson/GLab Dropbox/Anderson Brito/codes/geoCodes/bra_adm_ibge_2020_shp/bra_admbnda_adm2_ibge_2020.shp",
		cache = "config/cache_coordinates.tsv",
		index_column = "division_exposure",
		date_column = "date_testing",
		start_date = "2021-12-01",
		end_date = "2022-01-10"


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
		cache = arguments.cache,
		shapefile = arguments.shapefile,
		macros = "config/tabela_municipio_macsaud_estado_combined.tsv"
	params:
		geo_columns = "state, location",
		add_geo = "country:Brazil",
		lat = "lat",
		long = "long",
		check_match = "ADM2_PT",
		target = "ADM1_PT, ADM1_PCODE, ADM2_PT, ADM2_PCODE",
		target2 = "DS_UF_SIGLA, CO_MACSAUD, DS_NOMEPAD_macsaud",
		index = "ADM2_PCODE",
		action = "add",
		mode = "columns"
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
			--cache {input.cache} \
			--check-match {params.check_match} \
			--target \"{params.target}\" \
			--output {output.matrix}
		
		python3 scripts/reformat_dataframe.py \
			--input1 {output.matrix} \
			--input2 {input.macros} \
			--index {params.index} \
			--action {params.action} \
			--mode {params.mode} \
			--targets "{params.target2}" \
			--output {output.matrix}
		"""


rule detection:
	message:
		"""
		Aggregate data related to SGTF results
		"""
	input:
		input_file = "results/combined_testdata_geo.tsv"
	params:
		xvar = arguments.date_column,
		xtype = "time",
		format = "integer",
		
		yvar_country = "country S_detection",
		index_country = "country",
		
		yvar_macros = "CO_MACSAUD S_detection",
		index_macros = "CO_MACSAUD",
		extra_columns_macros = "DS_UF_SIGLA DS_NOMEPAD_macsaud",

		yvar_states = "ADM1_PCODE S_detection",
		index_states = "ADM1_PCODE",
		extra_columns_states = "ADM1_PT country",
		
		yvar_location = "ADM2_PCODE S_detection",
		index_location = "ADM2_PCODE",
		extra_columns_location = "ADM2_PT state",
		
		filters = "test_result:Positive",
		start_date = arguments.start_date,
		end_date = arguments.end_date
	output:
		matrix_country = "results/matrix_country_detection.tsv",
		matrix_macros = "results/matrix_macros_detection.tsv",
		matrix_states = "results/matrix_states_detection.tsv",
		matrix_location = "results/matrix_location_detection.tsv",
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_country} \
			--unique-id {params.index_country} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_country}

		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_macros} \
			--unique-id {params.index_macros} \
			--extra-columns  {params.extra_columns_macros} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_macros}
			
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_states} \
			--unique-id {params.index_states} \
			--extra-columns  {params.extra_columns_states} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_states}
			
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_location} \
			--unique-id {params.index_location} \
			--extra-columns  {params.extra_columns_location} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_location}
		"""


rule test_results:
	message:
		"""
		Aggregate counts of all SGTF tests (Postive and Negative)
		"""
	input:
		input_file = "results/combined_testdata_geo.tsv"
	params:
		xvar = arguments.date_column,
		xtype = "time",
		format = "integer",
		
		yvar_country = "country test_result",
		index_country = "country",
		
		yvar_states = "ADM1_PCODE test_result",
		index_states = "ADM1_PCODE",
		extra_columns_states = "ADM1_PT country",
		
		yvar_location = "ADM2_PCODE test_result",
		index_location = "ADM2_PCODE",
		extra_columns_location = "ADM2_PT state",
		
		start_date = arguments.start_date,
		end_date = arguments.end_date
	output:
		matrix_country = "results/matrix_country_posneg.tsv",
		matrix_states = "results/matrix_states_posneg.tsv",
		matrix_location = "results/matrix_location_posneg.tsv",
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_country} \
			--unique-id {params.index_country} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_country}

		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_states} \
			--unique-id {params.index_states} \
			--extra-columns  {params.extra_columns_states} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_states}
			
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_location} \
			--unique-id {params.index_location} \
			--extra-columns  {params.extra_columns_location} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_location}
		"""


rule aggregate:
	message:
		"""
		Aggregate data by week
		"""
	input:
		input_c1 = "results/matrix_country_detection.tsv",
		input_c2 = "results/matrix_country_positives.tsv",
		input_s1 = "results/matrix_states_detection.tsv",
		input_s2 = "results/matrix_states_positives.tsv"
	params:
		unit = "week",
		format = "integer"
	output:
		matrix_c1 = "results/matrix_country_detection_week.tsv",
		matrix_c2 = "results/matrix_country_positives_week.tsv",
		matrix_s1 = "results/matrix_states_detection_week.tsv",
		matrix_s2 = "results/matrix_states_positives_week.tsv"
	shell:
		"""
		python3 scripts/aggregator.py \
			--input {input.input_c1} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.matrix_c1}
		
		python3 scripts/aggregator.py \
			--input {input.input_c2} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.matrix_c2}
		
		python3 scripts/aggregator.py \
			--input {input.input_s1} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.matrix_s1}
		
		python3 scripts/aggregator.py \
			--input {input.input_s2} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.matrix_s2}
		"""



rule sgtf_percent:
	message:
		"""
		Percentage of STGF cases
		"""
	input:
		input_c1 = "results/matrix_country_detection.tsv",
		input_c2 = "results/matrix_country_positives.tsv",
		input_c3 = "results/matrix_country_detection_week.tsv",
		input_c4 = "results/matrix_country_positives_week.tsv",
		input_s1 = "results/matrix_states_detection.tsv",
		input_s2 = "results/matrix_states_positives.tsv",
		input_s3 = "results/matrix_states_detection_week.tsv",
		input_s4 = "results/matrix_states_positives_week.tsv",
	params:
		index_c1 = "country S_detection",
		index_c2 = "country",
		index_s1 = "ADM1_PCODE S_detection",
		index_s2 = "ADM1_PCODE"
	output:
		matrix_c1 = rules.all.input.sgtf_c1,
		matrix_c2 = rules.all.input.sgtf_c2,
		matrix_s1 = rules.all.input.sgtf_s1,
		matrix_s2 = rules.all.input.sgtf_s2,
	shell:
		"""
		python3 scripts/normdata.py \
			--input1 {input.input_c1} \
			--input2 {input.input_c2} \
			--index1 {params.index_c1} \
			--index2 {params.index_c2} \
			--output {output.matrix_c1}

		python3 scripts/normdata.py \
			--input1 {input.input_c3} \
			--input2 {input.input_c4} \
			--index1 {params.index_c1} \
			--index2 {params.index_c2} \
			--output {output.matrix_c2}

		python3 scripts/normdata.py \
			--input1 {input.input_s1} \
			--input2 {input.input_s2} \
			--index1 {params.index_s1} \
			--index2 {params.index_s2} \
			--output {output.matrix_s1}

		python3 scripts/normdata.py \
			--input1 {input.input_s3} \
			--input2 {input.input_s4} \
			--index1 {params.index_s1} \
			--index2 {params.index_s2} \
			--output {output.matrix_s2}
		"""


rule all_positives:
	message:
		"""
		Aggregate counts of all positive SGTF tests
		"""
	input:
		input_file = "results/combined_testdata_geo.tsv"
	params:
		xvar = arguments.date_column,
		xtype = "time",
		format = "integer",

		yvar_country = "country",
		index_country = "country",
		
		yvar_states = "ADM1_PCODE",
		index_states = "ADM1_PCODE",
		extra_columns_states = "ADM1_PT country",
		
		yvar_location = "ADM2_PCODE",
		index_location = "ADM2_PCODE",
		extra_columns_location = "ADM2_PT state",
		
		filters = "test_result:Positive",
		start_date = arguments.start_date,
		end_date = arguments.end_date
	output:
		matrix_country = rules.all.input.matrix_c,
		matrix_states = rules.all.input.matrix_s,
		matrix_location = rules.all.input.matrix_l,
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_country} \
			--unique-id {params.index_country} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_country}

		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_states} \
			--unique-id {params.index_states} \
			--extra-columns  {params.extra_columns_states} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_states}
			
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_location} \
			--unique-id {params.index_location} \
			--extra-columns  {params.extra_columns_location} \
			--filter {params.filters} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_location}
		"""

rule total_tests:
	message:
		"""
		Aggregate counts of total SGTF tests
		"""
	input:
		input_file = "results/combined_testdata_geo.tsv"
	params:
		xvar = arguments.date_column,
		xtype = "time",
		format = "integer",

		yvar_country = "country",
		index_country = "country",
		
		yvar_states = "ADM1_PCODE",
		index_states = "ADM1_PCODE",
		extra_columns_states = "ADM1_PT country",
		
		yvar_location = "ADM2_PCODE",
		index_location = "ADM2_PCODE",
		extra_columns_location = "ADM2_PT state",
		
		start_date = arguments.start_date,
		end_date = arguments.end_date
	output:
		matrix_country = "results/matrix_country_total.tsv",
		matrix_states = "results/matrix_states_total.tsv",
		matrix_location = "results/matrix_location_total.tsv",
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_country} \
			--unique-id {params.index_country} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_country}

		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_states} \
			--unique-id {params.index_states} \
			--extra-columns  {params.extra_columns_states} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_states}
			
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar_location} \
			--unique-id {params.index_location} \
			--extra-columns  {params.extra_columns_location} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_location}
		"""


rule positivity:
	message:
		"""
		Percentage of positive tests
		"""
	input:
		input_c1 = "results/matrix_country_posneg.tsv",
		input_c2 = "results/matrix_country_total.tsv",
		input_s1 = "results/matrix_states_posneg.tsv",
		input_s2 = "results/matrix_states_total.tsv",
	params:
		index_c1 = "country test_result",
		index_c2 = "country",
		index_s1 = "ADM1_PCODE test_result",
		index_s2 = "ADM1_PCODE"
	output:
		matrix_c1 = rules.all.input.posrate_c1,
		matrix_s1 = rules.all.input.posrate_s1,
	shell:
		"""
		python3 scripts/normdata.py \
			--input1 {input.input_c1} \
			--input2 {input.input_c2} \
			--index1 {params.index_c1} \
			--index2 {params.index_c2} \
			--output {output.matrix_c1}

		python3 scripts/normdata.py \
			--input1 {input.input_s1} \
			--input2 {input.input_s2} \
			--index1 {params.index_s1} \
			--index2 {params.index_s2} \
			--output {output.matrix_s1}
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
