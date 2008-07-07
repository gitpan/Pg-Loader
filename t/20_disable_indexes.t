package main;
BEGIN { push @ARGV, "--dbitest=32"}
use DBI;
use Pg::Loader::Query;
use Test::More qw( no_plan );
use Test::MockDBI;


*disable_indexes = \& Pg::Loader::Query::disable_indexes;

my $mock = get_instance Test::MockDBI;
my $dh   = DBI->connect('dbi:Pg:a');

ok $dh;

my $fake = [ { name=>'n_pkey', pk=>1, def=>'alter table e add primary key(c)'}, 
             { name=>'bb'    , pk=>0, def=>'create index bb on exam(fn,ln)'  }, 
];
$mock->set_retval_scalar(32,'.*select.*', $fake );

exit;


