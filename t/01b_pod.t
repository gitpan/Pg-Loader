use Test::More;


my $dir  = $ENV{PWD} =~ m#\/t$#  ? '../' : '';
my @files =   (    "${dir}blib/lib/Pg/Loader.pm" ,
		   "${dir}script/pgloader.pl",
);
plan  tests=> scalar @files;

eval 'use Test::Pod' ;


SKIP: {        
		skip  'no Test::Pod', scalar @files    if $@ ;
		pod_file_ok( $_,  $_)   for @files;
};

