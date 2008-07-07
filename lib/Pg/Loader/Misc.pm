# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .

package Pg::Loader::Misc;

use v5.10;
use Data::Dumper;
use strict;
use warnings;
use Config::Format::Ini;
use Text::CSV;
use Pg::Loader::Query;
use List::MoreUtils  qw( firstidx );
use Log::Log4perl  qw( :easy );
use base 'Exporter';
use Quantum::Superpositions ;



our $VERSION = '0.02';

our @EXPORT = qw(
	ini_conf	error_check   	fetch_options   show_sections
	usage		version		merge_with_template 
	print_results	l4p_config	add_defaults    subset
	error_check_pgsql               filter_ini      filter_val
	add_modules
);
our $o;
sub fetch_options {
        ($o = new Getopt::Compact
                args   => '[section]...',
                modes  => [qw(quiet debug verbose )],
		struct => [ [ [qw(c config)],  'config file', '=s'            ],
                            [ [qw(s summary)], 'summary'                      ],
			    [ [qw(n dry_run)], 'dry_run'                      ],
			    [ [qw(l loglevel)],'loglevel', '=i'               ],
			    [ [qw(D disable_triggers)],'disable triggers'     ],
			    [ [qw(T truncate)],'truncate table before loading'],
			    [ [qw(V vacuum)],  'vacuum analyze'               ],
			    [ [qw(C count)],   'num of lines to process','=s' ],
			    [ [qw(  version)],  'version'  , '', \&version    ],
			    [ [qw(F from)],    'process from this line number'],
			    [ [qw(i indexes)], 'disable indexes during COPY  '],
			])->opts;
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
        $s->{ filename   }   //=  'pgloader.dat'                    ;
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
	DEBUG("\tPassed error check");
}
sub subset {
	my ($h,$n) = @_ ;
        # True if $n is subset of $h;
        my @intersection = eigenstates(all( any(@$h), any(@$n) ));
	(@intersection == @$n);
}

sub filter_ini {
	my ($s, $dh) = @_ ;

	$s->{$_}     =~  s/ \\ (?=,) //gox      for keys %$s;
	$s->{format} =~  s/^ \s*'|'\s* $//xog;

        my $attributes = [ get_columns_names( $dh, $s->{table}, $s ) ];

        # $s->{copy} might be an arrayref
	$s->{copy}        =~ /^\s*[*]\s*$/ox and $s->{copy}       = $attributes;
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
sub filter_val {
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
