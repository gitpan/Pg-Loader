use Pg::Loader;
use Test::More qw( no_plan );

my $dir  = $ENV{PWD} =~ m#\/t$#  ? '' : 't/';


* _stop_input   = \& Pg::Loader::_stop_input;


ok ! _stop_input ( { count => 3 } , 2 );
ok ! _stop_input ( {            } , 3 );
ok ! _stop_input ( {            } , 0 );
ok ! _stop_input ( {            } ,   );

ok   _stop_input ( { count => 3 } , 4 );
ok   _stop_input ( { count => 3 } , 3 );
ok   _stop_input ( { count => 0 } , 0 );
ok   _stop_input ( { count => 0 } ,   );

