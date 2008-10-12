
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


our $VERSION = '0.04';

our @EXPORT = qw(
	ini_conf	error_check   	fetch_options   show_sections
	usage		version		merge_with_template 
	print_results	l4p_config	add_defaults    subset
	error_check_pgsql               filter_ini    
	add_modules     
);
our $o;
sub fetch_options {
        ($o = new Getopt::Compact
                args   => '[section]...',
                modes  => [qw(quiet debug verbose )],
		struct => [ [ [qw(c config)],  'config file', '=s'            ],
                            [ [qw(s summary)], 'summary'                      ],
                            [ [qw(t table)], 'schema.table'                   ],
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
	#copy_every=10000
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
         my $ini = read_ini shift||'pgloader.conf';
}

sub usage   { say $o->usage() and exit }
sub version { say $VERSION    and exit }

sub merge_with_template {
        my ( $ini, $section) = @_;
	$ini->{$section} || LOGDIE(qq(no config section for [$section]));
        my $template = $ini->{$section}{use_template} || return;
        return unless $ini->{$template}{template};
        DEBUG( "\tFound template for $section"  );
        while (my($k,$v) = each %{$ini->{$template}}) {
                $ini->{$section}{$k} //= $v ;
        }
}

sub print_results {
        my @stats = shift || return;
        printf "%-17s | %11s | %7s | %10s | %10s\n",
                'section name', 'duration', 'size', 'copy rows', 'errors';
        say '='x 68;
        printf "%-17s | %10.3fs | %7s | %10d | %10s\n" ,
          $_->{name}, $_->{elapsed}, '-', $_->{rows}, $_->{errors}  for @stats;
}

sub add_defaults {
	my ( $ini, $section) = @_ ;
	my $s      =  $ini->{$section}                       ;
	LOGDIE "invalid section name"  unless $section       ;
	LOGDIE "missing [$section]"    unless $s             ;

        merge_with_template( $ini, $section )                ;
        $s->{null} = 'NULL as $$'.($s->{null} //'\NA') .'$$'        ;
	$s->{ copy       }   //= '*'                                ;  
	$s->{ copy_every }   //=  10_000                            ;
        $s->{ filename   }   //=  'STDIN'                           ;
        $s->{ format     }   //=  'text'                            ;
        $s->{ null       }   //=  '$$\NA$$'                         ;
        $s->{ table      }   //=   $section                         ;
	$s->{ quotechar  }   //=  '"'                               ;

        my $is_text =  $s->{ format } =~ /^ '? text '?$/ox          ;
        $s->{ field_sep  }   //=  $is_text ? "\t" : ','             ;
}


sub error_check_pgsql  {
	my  ($conf, $ini) = @_ ;
	my $s = $ini->{pgsql} || LOGDIE(qq(no pgsql section ));
	if ($s->{pgsysconfdir} || $ENV{ PGSYSCONFDIR } ) {
		my $msg = 'expected service parameter in pgsql section';
		$s->{service} or LOGDIE ( $msg ) ;
	}
	$conf->{dry_run} //= 0;
}

sub error_check  {
	my ( $ini, $section) = @_;
	die unless $section;
	my $s = $ini->{$section}|| LOGDIE(qq(no config section for [$section]));
        my $msg01 = q("copy_columns" and "only_cols" are both defined);
        $s->{copy_columns} and $s->{only_cols} and LOGDIE( $msg01 ) ;

        $s->{filename}  or LOGDIE(qq(no filename specified for [$section]));
        $s->{table}     or LOGDIE(qq(no table    specified for [$section]));
        _check_copy( $s->{copy} );
	DEBUG("\tPassed error check");
}
sub _check_copy {
        my $values = shift||return;
        # $s->copy should be either a '*' string, or an array of
        # string in the form of  \w(:\d+) . Whitespaces will be cut.
	my $err = 'invadid value for param "copy"' ;

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
	my ($s, $dh) = @_ ;

	$s->{$_}     =~  s/ \\ (?=,) //gox      for keys %$s;
	$s->{format} =~  s/^ \s*'|'\s* $//xog;

        my $attributes = [ get_columns_names( $dh, $s->{table}, $s ) ];

	$s->{copy}  =  ($s->{copy}=~/^\s*[*]\s*$/ox) ?  $attributes 
                                                     : _copy_param $s->{copy};

	($s->{copy_columns}||'') =~/^\s*[*]\s*$/ox 
                                             and $s->{copy_columns}=$attributes;

	ref $s->{copy}         or $s->{copy}         = [$s->{copy}];
	if ($s->{copy_columns}) {
		ref $s->{copy_columns} or 
		$s->{copy_columns} = [$s->{copy_columns}];
	}
	subset $attributes,$s->{copy} or LOGDIE q(invalid values for "copy"');
	subset( $attributes, $s->{copy_columns}) or
                          LOGDIE  q(invalid values for copy_columns');

	given ($s->{format} ) {
		when (/^(text|csv)$/)  {} ;
		default   { LOGDIE( q(format must be 'text' or 'csv')) };
	};
	
        (ref $s->{reformat} eq 'ARRAY') or $s->{reformat} = [$s->{reformat} ];
	
	if ( $s->{reformat} ) {
		for ( @{$s->{reformat}} ) {
			next unless $_;
			my ($col, $mod, $fun ) = m/^(\w+): (.*)::(\w+) $/gxo;
			next unless defined $fun;
			$s->{rfm}{$col} = { col=>$col, pack=>$mod, fun=>$fun };
		}
        }
	$s->{attributes} = $attributes;
	DEBUG("\tPassed filter");
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
