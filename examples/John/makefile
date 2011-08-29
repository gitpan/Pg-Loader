.PHONY: $(shell ls)
.SILENT:

#args= -n 3  #-dv   # -c /bin/touch  -p pid

MODULES  =  -MData::Dumper    -Mstrict -Mblib=../blib

OPTS     =  -lw
args     = -c e.conf -TV  -si   exam 

all: try

try e*:
	perl $(OPTS) $(MODULES) $@.pl
