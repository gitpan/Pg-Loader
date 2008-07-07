.PHONY: $(shell ls)
.SILENT: 

#   00_load.t         		01b_pod.t             01c_coverage.t  
#   02_range2list     		03_schema_name.t      04_query_basic.t
#   05_field_nums_reqe.t        06_requested_cols.t       
#   10_simplify_ini.t           11_ini_conf.t         12_error_check.t
#   13_subset.t
#   20_disable_indexes.t        21_enable_indexes.t
#   31_add_defaults.t           32_filter_ini.t
 
MODULES  =  -MData::Dumper -MCarp  
MODULES +=  -MTest::More   -Mblib=../blib -Mv5.10 

#all: 01c_coverage.t
#all: 12_error_check.t
#all: 12_error_check.t
#all: 32_filter_ini.t
all: 06_requested_cols.t

3* 2* 1* 0*:
	perl -w -Mstrict -Mblib  $(MODULES)   $@  
cp:
	cp  ../blib/lib/Pg/Loader.pm    ../lib/Pg
	cp  ../blib/lib/Pg/Loader/*.pm  ../lib/Pg/Loader/
w:
	chmod -R u+w  ../blib/lib/Pg
vil: w
	vim  ../blib/lib/Pg/Loader.pm  
viq: w
	vim  ../blib/lib/Pg/Loader/Query.pm  
vim: w
	vim  ../blib/lib/Pg/Loader/Misc.pm  
vic: w
	vim  ../blib/lib/Pg/Loader/Columns.pm  
vi: w
	vim  ../script/pgloader.pl
pod:
	perldoc ../script/pgloader.pl
podl:
	perldoc ../blib/lib/Pg/Loader.pm

