use Pg::Loader::Misc;
use Test::More qw( no_plan );
use Test::Exception;


*add_defaults = \& Pg::Loader::Misc::add_defaults;

my $m = { apple  => {  } };
my $s = { apple  => { null => 'na', use_template=>undef } ,
          fruit  => { }, };
my $a = { apple  => { use_template=>'fruit' } ,
          fruit  => { template=>1, format=>'csv', null=>'aa'}, };
my $b = { apple  => { use_template=>'fruit' } ,
          fruit  => { template=>undef, only_cols=>1, 
                      format=>'csv', null=>'bb'}, };

add_defaults( $m, 'apple');  my $mm = $m->{apple};
add_defaults( $s, 'apple');  my $ss = $s->{apple};
add_defaults( $a, 'apple');  my $aa = $a->{apple};
add_defaults( $b, 'apple');  my $bb = $b->{apple};

is_deeply [ @{$aa}{qw( format copy copy_columns)}], [ qw( csv * *) ];
is_deeply [ @{$aa}{qw( copy_every filename     )}], [ qw( 10000 STDIN )];
is_deeply [ @{$aa}{qw( field_sep)}], [ ',' ];


is   $aa->{ null   }   ,  'NULL as $$aa$$' ;
is   $aa->{ format }   ,  'csv'            ;

is_deeply [ @{$bb}{qw( table format only_cols )}], 
          [        qw( apple csv 1)       ];

my $fields = [qw( copy copy_columns copy_every field_sep 
                  filename format null quotechar table 
             )];
#TODO
#is_deeply [ sort keys %{$m->{apple}} ] , $fields ;
#is scalar (values %{$m->{apple}}), @$fields; 

is_deeply [ @{$mm}{qw( copy copy_every filename      format table)}],
	  [        qw( *    10000      STDIN         text    apple)];

is   $bb->{ null }  ,  'NULL as $$bb$$' ;
is   $mm->{ null }  ,  'NULL as $$\NA$$' ;
is   $ss->{ null }  ,  'NULL as $$na$$'  ;

dies_ok { add_defaults ( $s, 'appl') };
dies_ok { add_defaults ( $s, '') };
dies_ok { add_defaults ( $s, undef) };
