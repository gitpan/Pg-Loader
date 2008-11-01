# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .

package Pg::Loader::Misc;

use v5.10;
use Data::Dumper;
use strict;
use warnings;
use Config::Format::Ini;
use Log::Log4perl ':easy';
use Text::CSV;
use Pg::Loader::Columns;
use Pg::Loader::Query;
use Pg::Loader::Log;
use List::MoreUtils  qw( firstidx );
use base 'Exporter';
use Quantum::Superpositions ;
use Text::Table;

*get_columns_names = \&Pg::Loader::Query::get_columns_names;


our $VERSION = '0.12';

our @EXPORT = qw(
	ini_conf	error_check   	fetch_options   show_sections
	usage		version		merge_with_template 
	print_results	add_defaults    subset
	error_check_pgsql               filter_ini      reformat_values
	add_modules     pk4updates      
	insert_semantic_check           update_semantic_check
);
our $o;
sub fetch_options {
        ($o = new Getopt::Compact
                args   => '[section]...',
                modes  => [qw(quiet debug verbose )],
		struct => [ [ [qw(c config)],  'config file',   '=s'          ],
                            [ [qw(t relation)],'schema.table' , '=s'          ],
			    [ [qw(l loglevel)],'loglevel',      '=i'          ],
			    [ [qw(L Logfile)], 'Logfile',       '=s'          ],
                            [ [qw(s summary)], 'summary'                      ],
                            [ [qw(e every)],   'sets  copy_every=1'           ],
			    [ [qw(n dry_run)], 'dry_run'                      ],
			    [ [qw(D disable_triggers)],'disable triggers'     ],
			    [ [qw(T truncate)],'truncate table before loading'],
			    [ [qw(V vacuum)],  'vacuum analyze'               ],
			    [ [qw(C count)],   'num of lines to process','=s' ],
			    [ [qw(  version)],  'version'  , '', \&version    ],
			    [ [qw(F from)],    'process from this line number'],
			    [ [qw(i indexes)], 'disable indexes during COPY  '],
			    [ [qw(g generate)],'generate conf file','!',\&gen ],
			])->opts;
}
sub  gen {
	(my $tmp = <<EOM ) =~ s/^\t//gmo ;
	[pgsql]
	base  = people
	pass  = apple
	#host = localhost
	#pgsysconfdir=.
	#service=

	[exam]
	filename      = exam.dat
	table         = public.exam
	#copy         = *
	#copy_columns = id, name
	#only_cols    = 1-2,4,5
	#use_template = cvs1
	#copy_every=10000

	[cvs1]
	#template=true
	#format=cvs
	#doublequote=false
	#escapechar=|
	#quotechar="
	#skipinitialspace=true
	#reject_log=rej_log
	#reject_data=rej_data
	#reformat= fn:John::Util::jupper, score:John::Util::changed
	#null=\\NA
	#trailing_sep=true
	#datestyle=euro
	#client_encoding=
	#lc_messages=C
	#lc_numeric=C
	#lc_monetary=en_US
	#lc_type=C
	#lc_time=POSIX

EOM
	say $tmp; exit;
}



sub ini_conf {
	$Config::Format::Ini::SIMPLIFY = 1 ;
	my $file = shift || 'pgloader.conf';
        INFO( "Configuring from $file" )   ;
	my $ini = read_ini $file           ;
}

sub usage   { say $o->usage() and exit }
sub version { say 'pgloader.pl version '. $VERSION    and exit }


sub print_results {
        my @stats = @_ or return;
        printf "%-17s | %11s | %7s | %10s | %10s\n",
                'section name', 'duration', 'size', 'affected rows', 'errors';
        say '='x 70;
        printf "%-17s | %10.3fs | %7s | %13d | %10s\n" ,
		@{$_}{qw( name elapsed size rows errors)}  for @stats;
}

sub merge_with_template {
        ## Output: add columns into $s
        my ( $s, $ini, $template) = @_;
	return                                  unless $template;
	LOGEXIT "Missing template [$template]"  unless $ini->{$template};
        $s->{$_} //= $ini->{$template}{$_}      for  keys %{$ini->{$template}};
}

sub add_defaults {
	my ( $ini, $section) = @_ ;
	LOGEXIT "invalid section name"              unless $section    ;
	my $s      =  $ini->{$section}                                 ;
	LOGEXIT "Missing section [$section]"        unless $s          ;
        merge_with_template( $s, $ini, $s->{use_template} ) ;
	_switch_2_update( $s );

        $s->{ format      }   //=  'text'                            ;
	$s->{ copy        }   //= '*'                                ;  
	$s->{ copy_every  }   //=  10_000                            ;
        $s->{ filename    }   //=  'STDIN'                           ;
        $s->{ table       }   //=   $section                         ;
	$s->{ quotechar   }   //=  '"'                               ;
	$s->{ reject_data }   //=   ''                               ;
	$s->{ reject_log  }   //=   ''                               ;
	$s->{ lc_messages }   //=   ''                               ;
	$s->{ lc_numeric  }   //=   ''                               ;
	$s->{ lc_monetary }   //=   ''                               ;
	$s->{ lc_type     }   //=   ''                               ;
	$s->{ lc_time     }   //=   ''                               ;
	$s->{ datestyle   }   //=   ''                               ;
	$s->{ client_encoding } //= ''                               ;

	$s->{copy_columns} = $s->{copy} 
                        unless ($s->{only_cols}||$s->{copy_columns});

	if (  $s->{ format } =~ /^ '? text '?$/ox  ) { 
	        # format is 'text'
                $s->{null} //= '$$\NA$$'            ;
		$s->{null}   = '$$'.$s->{null}.'$$' if $s->{null} ne '$$\NA$$';
		$s->{ field_sep  }   //=  "\t"      ;
	}else{
	        # format is 'csv'
		$s->{null} //= '$$$$'               ;
		$s->{null}   = '$$'.$s->{null}.'$$' if  $s->{null} ne '$$$$';
		$s->{ field_sep  }   //= ','        ;
	}
}


sub error_check_pgsql  {
	my  ($conf, $ini) = @_ ;
	my $s = $ini->{pgsql} || LOGEXIT(qq(Missing pgsql section ));
	if ($s->{pgsysconfdir} || $ENV{ PGSYSCONFDIR } ) {
		my $msg = 'Expected service parameter in pgsql section';
		$s->{service} or LOGEXIT ( $msg ) ;
	}
	$conf->{dry_run} //= 0;
}

sub error_check  {
	my ( $ini, $section) = @_;
	die unless $section;
	my $s = $ini->{$section}|| LOGEXIT(qq(No config for [$section]));
        my $msg01 = q("copy_columns" and "only_cols" are mutually exclusive);
        $s->{copy_columns} and $s->{only_cols} and LOGEXIT( $msg01 ) ;

        $s->{filename}  or LOGEXIT(qq(No filename specified for [$section]));
        $s->{table}     or LOGEXIT(qq(No table specified for [$section]));
	$s->{format} =~  s/^ \s*'|'\s* $//xog;
        $s->{format}    or LOGEXIT(qq(No format specified for [$section]));
	given ($s->{format} ) {
		when (/^(text|csv)$/)  {} ;
		default   { LOGEXIT( q(Set format to either 'text' or 'csv'))};
	}; 
        _check_copy_grammar( $s->{copy} );
	DEBUG("\tPassed grammar check");
}
sub _switch_2_update {
	my  $s  = shift;;
	# First, some error checking
	eval {
		$s->{ copy         } && $s->{ update         }  and die;
		$s->{ copy         } && $s->{ update_columns }  and die;
		$s->{ copy         } && $s->{ update_only    }  and die;
		$s->{ copy_columns } && $s->{ update         }  and die;
		$s->{ copy_columns } && $s->{ update_columns }  and die;
		$s->{ copy_columns } && $s->{ update_only    }  and die;
		$s->{ copy_only    } && $s->{ update         }  and die;
		$s->{ copy_only    } && $s->{ update_columns }  and die;
		$s->{ copy_only    } && $s->{ update_only    }  and die;
	1;
	} or  LOGDIE  qq(\tCannot mix "copy" with "update" columns) ;
	#TODO "update_only" should populate "update_columns"
	$s->{ update_only } and  LOGDIE qq(\t"update_only" not implemeted");
	# Should we switch to update mode ?
	exists $s->{ update_columns }  and  $s->{mode}='update';
        exists $s->{ update_only }     and  $s->{mode}='update';
        exists $s->{ update }          and  $s->{mode}='update';
        ## key statement
	$s->{mode} //= 'copy' ;
	if ($s->{mode} eq 'update') {
		$s->{copy_columns} = $s->{update_columns}//$s->{update};
		$s->{copy_only}    = $s->{update_columns}//'';
	}
	
}
sub  pk4updates {
	## ensure that table has pk
        ## Output: the pk as an arrayref
	my ($dh, $s) = @_ ;
	my $table = $s->{table};
        $s->{pk}  =  Pg::Loader::Query::primary_keys($dh, $table);
	@{$s->{pk}} ;
}
sub update_semantic_check {
        my $s = shift;
	# ensure "copy_columns" is an arrayref
        my $msg = qq(\t"update_columns" cannot contain primary keys);
	for my $pk (@{$s->{pk}}) {
        	grep   { /^$pk$/ } @{$s->{copy_columns}}  and LOGDIE( $msg ); 
	}
        $msg = qq(\t"update" must include the primary keys);
        subset( $s->{copy}, $s->{pk} )          or  LOGDIE(  $msg );
        $msg = qq(\tConfig implies that no columns should be updated);
	( @{$s->{copy}}  == @{$s->{pk}} )  and LOGDIE( $msg );
	#TODO: what is updated must be in the "copy" list
}
sub _check_copy_grammar {
        my $values = shift||return;
        # $s->copy should be either a '*' string, or an array of
        # string in the form of  \w(:\d+) . Whitespaces are trimed.
	my $err = 'Invadid value for param "copy"' ;

        if (ref $values eq 'ARRAY') {
		# array of arrayref
        	my $max =  $#{$values};
		my $pat =  qr/^\s*\w+(?:\s*[:]\s*\d+)?/   ;
            	($max+1) == grep { LOGEXIT  $err  unless $_;
				   LOGEXIT  $err  unless $_=~ $pat;
                                 } @$values  or  LOGEXIT $err;
	}else{
		# assume it is string, big assumption
		my $_   =  $values ;
		my $pat =  qr/^ \s* \w+ (?:[:]1)? \s* $/xo;
		LOGEXIT $err unless (/^ \s* [*] \s* $/xo  or $_=~ $pat );
	}
	# passed 
}

sub subset {
	my ($h,$n) = @_ ;
        # True if $n is subset of $h;
        my @intersection = eigenstates(all( any(@$h), any(@$n) ));
	(@intersection == @$n);
}
sub _copy_param {
        my $values = shift;
        # receives a array of strings like [qw(a:1 b c:4 d:3)] and returns
        # an arrayref of ordered columns: [q( a b d c )]
        return if $values =~ /^ \s* [*] \s* $/xo;

        (ref $values eq 'ARRAY') or  $values = [$values];

        my  ($max, $last, @ret) = ($#{$values}, 0);
        for (@$values) {
                s/^\s*|\s*$//og;
                my ($name, $num) =  split /\s*:\s*/, $_;
                $num //= $last+1;
                $last = $num;
                $ret[$num-1] = $name ;
        }
        LOGEXIT "invalid values for copy param"  unless $#ret == $max;
        \@ret;
}


sub filter_ini {
        # Check if configuration values are sensible. 
	# Assumption: The configuration syntax obeys grammar
	# Output: records real table attributes to $s->{attributes}
	# Output: "copy" and "copy_columns" become arrayrefs
	#TODO: parameters for "copy" should match those of actual table
	#TODO: parameters for "copy_only" should match those of actual table
	my ($s, $dh) = @_ ;

	$s->{$_} =~  s/ \\ (?=,) //gox      for keys %$s;

        my $attributes   = [ get_columns_names( $dh, $s->{table}, $s ) ];
	$s->{attributes} = $attributes;
	LOGEXIT("Could not fetch column names from db for table $s->{table}")  
				      unless @$attributes;

	$s->{copy}  =  ($s->{copy}=~/^\s*[*]\s*$/ox) ?  $attributes 
                                                     : _copy_param $s->{copy};
	($s->{copy_columns}||'') =~/^\s*[*]\s*$/ox 
                                             and $s->{copy_columns}=$s->{copy};
	# Ensure that "copy" and "copy_columns" are always arrayref
	ref $s->{copy}         or $s->{copy} = [$s->{copy}];
	ref $s->{copy_columns} or $s->{copy_columns} = [$s->{copy_columns}];
	DEBUG("\tPassed semantic check");
}

sub insert_semantic_check {
	my  $s = shift;
	# Check semantics for these things:
        # 1. "copy" is a subset of the real attribute names
        # 2. "copy" is a subset of the real attribute names
        # 3. "copy_only" is a subset of "copy"
	my $cmsg = q(names in "copy" are not a subset of actual table names);
	subset $s->{attributes}, $s->{copy}           or LOGEXIT( $cmsg );
	$cmsg= q(names in "copy_columns" are not a subset for actual names);
	subset( $s->{attributes}, $s->{copy_columns}) or LOGEXIT(  $cmsg );
	$cmsg= q(names in "copy_columns" are not a subset of "copy");
	subset( $s->{copy}, $s->{copy_columns})  or LOGEXIT(  $cmsg );
	#TODO: what is copied must be in the "copy" list
}

sub reformat_values {
	# Adjusts values as needed.
	# Assumption: The configuration syntax obay grammar
	# Output: TODO
	my ($s, $dh) = @_ ;
	return unless $s->{ reformat} ;
        (ref $s->{reformat} eq 'ARRAY') or $s->{reformat} = [$s->{reformat} ];
	for ( @{$s->{reformat}} ) {
		next unless $_;
		my ($col, $mod, $fun ) = m/^(\w+): (.*)::(\w+) $/gxo;
		next unless defined $fun;
		$s->{rfm}{$col} = { col=>$col, pack=>$mod, fun=>$fun };
	}
	DEBUG("\tPassed reformat");
}

sub add_modules {
	my $s = shift ;
	return unless $s->{rfm} ;
	for ( keys %{$s->{rfm}}) {
		my $h    = $s->{rfm}{$_};
		my ($pack, $fun) = @{$h}{'pack','fun'};
		(my $module = $pack) =~ s{::}{\/}o ;
		$module .= '.pm';
		require $module ;
		#say "${pack}::$fun";
		LOGEXIT  qq(could not find "${pack}::$fun") 
                                    unless  UNIVERSAL::can( $pack, $fun );
		$h->{ref} = 1   ;# cludge fix
	}
}

sub show_sections {
	my ($conf, $ini) = @_;
	my $port = ':'. ($ini->{port}||5432)  ;
	my $t    = new Text::Table  'SECTION   '  , 'TABLE    ', 
                                    'FILE      '  , 'OPERATION' ;
	DEBUG  "$ini->{pgsql}{base}\@$ini->{pgsql}{host}$port"   ;
	while (my ($k,$v) = each %$ini) {
		my $s = $ini->{$k};
		next if $k eq 'pgsql';
		next if exists $s->{template};
		next if $s->{template};
		add_defaults  $ini, $k ;
                _switch_2_update $s ;
		my $file = $s->{filename}||'STDIN';
		#say  sprintf  '%-18s %-20s', "[$k]", $file ;
		$t->load( [ "[$k]", $s->{table}, $file,  $s->{mode} ] );
	}
	print $t;
}

1;
__END__
