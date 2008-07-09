use Pg::Loader::Columns;
use Test::More qw( no_plan );
use Test::Exception;

*combine  = \&Pg::Loader::Columns::combine  ;
*init_csv = \&Pg::Loader::Columns::init_csv ;

my ($s, $csv, $data);
$data = { name=>'john', age=>'33', race=>'white' };

$s    = { field_sep => ',' };
$csv  = init_csv( $s );
is_deeply [ combine($s, $csv, $data, qw( name age     ))], ['john,33'] ;

$s   = { udc_name  => 'ano' };
$csv = init_csv( $s );
is_deeply [ combine($s, $csv, $data, qw( name age race))], ['ano33white'] ;

$s   = { udc_name  => 'ano', udc_no => 'no' };
$csv = init_csv( $s );
is_deeply [ combine($s, $csv, $data, qw( name age race))], ['ano33white'] ;


$data = { name=>'john', age=>'33', race=>'white' };
$s    = { field_sep => ','  , rfm=>{ name => { pack=>'any module',
                                               fun => 'any name',
                                               ref => sub{ uc shift},
                                              },}
};
$csv  = init_csv( $s );
is_deeply [ combine($s, $csv, $data, qw( name age race))], ['JOHN,33,white'];

# error check
is_deeply [ combine($s,$csv, $data, qw( ))], [''] ;


#ok ! combine($s,$csv, $data, undef );
