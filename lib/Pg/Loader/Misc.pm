
# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .

package Pg::Loader::Misc;

use v5.10;
use Data::Dumper;
use strict;
use warnings;
use Config::Format::Ini;
use Text::CSV;
use Pg::Loader::Columns;
use List::MoreUtils  qw( firstidx );
use Log::Log4perl  qw( :easy );
use base 'Exporter';
use Quantum::Superpositions ;

*get_columns_names = \&Pg::Loader::Query::get_columns_names;


our $VERSION = '0.10';

our @EXPORT = qw(
	ini_conf	error_check   	fetch_options   show_sections
	usage		version		merge_with_template 
	print_results	l4p_config	add_defaults    subset
	error_check_pgsql               filter_ini      reformat_values
	add_modules     
);
our $o;
sub fetch_options {
        ($o = new Getopt::Compact
                args   => '[section]...',
                modes  => [qw(quiet debug verbose )],
		struct => [ [ [qw(c config)],  'config file', '=s'            ],
                            [ [qw(s summary)], 'summary'                      ],
                            [ [qw(t table)],   'schema.table'                 ],
			    [ [qw(u update)],  'update'                       ],
			    [ [qw(n dry_run)], 'dry_run'                      ],
			    [ [qw(l loglevel)],'loglevel', '=i'               ],
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
	#lc_messages=
	#lc_numeric=

EOM
	print $tmp; exit;
}

sub l4p_config {
	my $c     = shift || return;
	$c->{loglevel} //= 2;
        $c->{loglevel} < 1  and $c->{loglevel} = 1;
        $c->{loglevel} > 4  and $c->{loglevel} = 4;
        $c->{verbose}       and $c->{loglevel} = 3;
        $c->{debug}         and $c->{loglevel} = 4;
        $c->{quiet}         and $c->{loglevel} = 1;
	my $level = (5-$c->{loglevel})*10_000 ;

	Log::Log4perl->easy_init( { level    => $level               ,
				    file     => '>> /tmp/out.txt'    ,
				    category => 'Bar::Twix'          ,
				    layout   => '%F{1}-%L-%M: %m%n'  ,
				   }, 
                                   { level    => $level              ,
				     file     => 'STDOUT'            ,
				     category => ''                  ,
				     layout   => '%m%n'              ,
				   },
	);
}


sub ini_conf {
	$Config::Format::Ini::SIMPLIFY = 1;
	my $file = shift ||'pgloader.conf';
        INFO( "Configuring from $file" );
	my $ini = read_ini $file ;
}

sub usage   { say $o->usage() and exit }
sub version { say $VERSION    and exit }


sub print_results {
        my @stats = shift || return;
        printf "%-17s | %11s | %7s | %10s | %10s\n",
                'section name', 'duration', 'size', 'copy rows', 'errors';
        say '='x 68;
        printf "%-17s | %10.3fs | %7s | %10d | %10s\n" ,
          $_->{name}, $_->{elapsed}, '-', $_->{rows}, $_->{errors}  for @stats;
}

sub merge_with_template {
        ## Output: add columns into $s
        my ( $s, $ini, $template) = @_;
	return                                 unless $template;
	LOGDIE "Missing template [$template]"  unless $ini->{$template};
        $s->{$_} //= $ini->{$template}{$_}     for  keys %{$ini->{$template}};
}

sub add_defaults {
	my ( $ini, $section) = @_ ;
	LOGDIE "invalid section name"              unless $section    ;
	my $s      =  $ini->{$section}                                ;
	LOGDIE "Missing section [$section]"        unless $s          ;
        merge_with_template( $s, $ini, $s->{use_template} ) ;

        $s->{null} = 'NULL as $$'.($s->{null} //'\NA') .'$$'         ;
	$s->{ copy        }   //= '*'                                ;  
	$s->{ copy_every  }   //=  10_000                            ;
        $s->{ filename    }   //=  'STDIN'                           ;
        $s->{ format      }   //=  'text'                            ;
        $s->{ null        }   //=  '$$\NA$$'                         ;
        $s->{ table       }   //=   $section                         ;
	$s->{ quotechar   }   //=  '"'                               ;
	$s->{ lc_messages }   //=   ''                               ;
	$s->{ lc_numeric  }   //=   ''                               ;
	$s->{ datestyle   }   //=   ''                               ;
	$s->{ client_encoding } //= ''                               ;

	$s->{copy_columns} = $s->{copy} 
                        unless ($s->{only_cols}||$s->{copy_columns});

        my $is_text =  $s->{ format } =~ /^ '? text '?$/ox          ;
        $s->{ field_sep  }   //=  $is_text ? "\t" : ','             ;
}


sub error_check_pgsql  {
	my  ($conf, $ini) = @_ ;
	my $s = $ini->{pgsql} || LOGEXIT(qq(Missing pgsql section ));
	if ($s->{pgsysconfdir} || $ENV{ PGSYSCONFDIR } ) {
		my $msg = 'Expected service parameter in pgsql section';
		$s->{service} or LOGDIE ( $msg ) ;
	}
	$conf->{dry_run} //= 0;
}

sub error_check  {
	
	my ( $ini, $section) = @_;
	die unless $section;
	my $s = $ini->{$section}|| LOGDIE(qq(No config section for [$section]));
        my $msg01 = q("copy_columns" and "only_cols" are mutually exclusive);
        $s->{copy_columns} and $s->{only_cols} and LOGEXIT( $msg01 ) ;

        $s->{filename}  or LOGEXIT(qq(No filename specified for [$section]));
        $s->{table}     or LOGEXIT(qq(No table specified for [$section]));
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
sub _check_copy_grammar {
        my $values = shift||return;
        # $s->copy should be either a '*' string, or an array of
        # string in the form of  \w(:\d+) . Whitespaces are trimed.
	my $err = 'Invadid value for param "copy"' ;

        if (ref $values eq 'ARRAY') {
		# array of arrayref
        	my $max =  $#{$values};
		my $pat =  qr/^\s*\w+(?:\s*[:]\s*\d+)?/   ;
            	($max+1) == grep { LOGDIE  $err  unless $_;
				   LOGDIE  $err  unless $_=~ $pat;
                                 } @$values  or  LOGDIE $err;
	}else{
		# assume it is string, big assumption
		my $_   =  $values ;
		my $pat =  qr/^ \s* \w+ (?:[:]1)? \s* $/xo;
		LOGDIE $err unless (/^ \s* [*] \s* $/xo  or $_=~ $pat );
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
        LOGDIE "invalid values for copy param"  unless $#ret == $max;
        \@ret;
}


sub filter_ini {
        # Checks if configuration values are sensible. 
	# Assumption: The configuration syntax obeys grammar
	# Output: records real table attributes to $s->{attributes}
	# Output: "copy" and "copy_columns" become arrayrefs
	#TODO: parameters for "copy" should match those of actual table
	#TODO: parameters for "copy_only" should match those of actual table
	my ($s, $dh) = @_ ;

	#say  "$_  =>", $s->{$_}  for keys %$s; exit;
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

	# Check semantics for these things:
        # 1. "copy" is a subset of the real attribute names
        # 2. "copy" is a subset of the real attribute names
        # 3. "copy_only" is a subset of "copy"
	my $cmsg = q(names in "copy" are not a subset of actual table names);
	subset $attributes, $s->{copy}           or LOGEXIT( $cmsg );
	$cmsg= q(names in "copy_columns" are not a subset for actual names);
	subset( $attributes, $s->{copy_columns}) or LOGEXIT(  $cmsg );
	$cmsg= q(names in "copy_columns" are not a subset of "copy");
	subset( $s->{copy}, $s->{copy_columns})  or LOGEXIT(  $cmsg );
	DEBUG("\tPassed semantic check");
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
		$h->{ref} = UNIVERSAL::can( $pack, $fun );
		$h->{ref} or LOGDIE  qq(could not find "${pack}::$fun")  ;
	}
}

sub show_sections {
	my ($conf, $ini) = @_;
	my $port = ':'. ($ini->{port}||5432)  ;
	DEBUG  "$ini->{pgsql}{base}\@$ini->{pgsql}{host}$port"   ;
	while (my ($k,$v) = each %$ini) {
		next if $k eq 'pgsql';
		next if exists $ini->{$k}{template};
		my $file = $ini->{$k}{filename};
		say  "[$k]      $ini->{$k}{filename}" ;
	}
}

1;
__END__
