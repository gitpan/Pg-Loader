# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .

package Pg::Loader;

use v5.10;
use DBI;
use Fatal qw(open);
use Getopt::Compact;
use Data::Dumper;
use Time::HiRes qw( gettimeofday tv_interval );
use strict;
use warnings;
use Pg::Loader::Query;
use Pg::Loader::Misc;
use Pg::Loader::Columns;
use Log::Log4perl  qw( :easy );
use base 'Exporter';

our $VERSION = '0.07';

our @EXPORT = qw( loader  );

sub every {
	my ($s, $dh, $col, $csv, $fd, $conf, $defs, @col) = @_;
	my ( $format, $null, $table) = @{$s}{'format','null','table'};
	my ($dry , $r )= ( $conf->{dry}, $s->{copy_every} );

	my $sql = ($format eq 'text') 
			?  "COPY $table $col FROM STDIN $null"
			:  "COPY $table $col FROM STDIN CSV"  ;
	DEBUG( "\t\t$sql" )                                   ;
	$dh->do( $sql )  if (! $dry)                          ;

	state ($rows, $errors) ;
	my $data;
	while ( $r -- ) {
	        $data = $csv->getline_hr ($fd) ;
		last unless $data;
		last if _stop_input($conf, $rows//0)               ;
		$_     = combine( $s, $csv, $data, @col )         ;
		DEBUG( "\t\t$_" )                                 ;
		$rows += $dh->pg_putcopydata("$_\n") unless $dry  ;
	}
	if ((! $dry) and $dh->pg_putcopyend() ) {
                enable_indexes( $dh, $table, $defs )  if $conf->{indexes};
		$dh->commit ;
	}else{
		$dh->rollback; $errors++; $rows=0 ;
	}
	($rows, $errors//0, $data);
}
sub _stop_input {
	my ($conf, $rows) = @_ ;
	($rows//0) >= ($conf->{count} // '1E10')   ;
}

sub loader {
	my ( $conf, $ini, $dh, $section ) = @_                 ;
	my   $s = $ini->{$section}                             ;
	INFO("Processing [$section]")                          ;

	add_defaults( $ini, $section  )                        ;
	error_check(  $ini, $section  )                        ;
	filter_ini(   $ini->{$section}, $dh )                  ;
	add_modules(  $ini->{$section}, $dh )                  ;
	my $dry = $conf->{dry_run}                             ;

	my ($file, $format, $null, $table) = 
                    @{$s}{'filename','format','null','table'};
	my ($col, @col) = requested_cols( $s )               ;
	open my ($fd), $file                                 ;
	INFO("\tReading from $file")                         ;
	my $csv = init_csv( $s )                             ;
	$csv->column_names( @{$s->{copy}}  )                 ;
	my ($t0, $rows, $errors, $data) =  ([gettimeofday], 0, 0,'true')  ;

	$dh->begin_work;
	_truncate( $dh, $table, $dry )             if $conf->{truncate};
	_disable_triggers( $dh, $table, $dry)      if $conf->{disable_triggers};
	my $defs ; 
	$defs = _disable_indexes( $dh, $table)     if $conf->{indexes};

	while ( 1 ) {
		($rows, $errors, $data)= 
		       every($s, $dh, $col, $csv, $fd, $conf, $defs, @col);
		last unless $data                                         ;
		last if _stop_input($conf, $rows//0)                      ;
	}

	vacuum_analyze( $dh, $table, $dry )        if $conf->{vacuum};

	{ name => $section, elapsed => tv_interval($t0), 
	  rows => $rows,     errors => $errors 
        }
}
sub _truncate {
	my ($dh, $table, $dry) = @_  ;
	INFO("\tTruncating $table")                ;
	$dh->do("truncate $table")   unless $dry   ;
}
sub _disable_triggers {
	my ($dh, $table, $dry) = @_  ;
	DEBUG( "\tDisabling triggers")             ;
	$dh->do( <<"")                unless $dry  ;
	ALTER TABLE $table DISABLE TRIGGER ALL

}
sub _disable_indexes { 
	my ($dh, $table, $dry) = @_  ;
	disable_indexes( $dh, $table ) unless $dry   
}


1;
__END__

=over

=item dist_abstract

=back

Perl extension for loading Postgres tables


=head1 NAME

Pg::Loader - Perl extension for loading Postgres tables

=head1 SYNOPSIS

  use Pg::Loader;

=head1 DESCRIPTION

This is a helper module for pgloader.pl(1), which loads tables to 
a Postgres database. It is similar in function to the pgloader(1) 
python program (written by other authors).

=head2 EXPORT

Pg::Loader - Perl extension for loading Postgres tables


=head1 SEE ALSO

http://pgfoundry.org/projects/pgloader/  hosts the original python
project.


=head1 AUTHOR

Ioannis Tambouras, E<lt>ioannis@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 by Ioannis Tambouras

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
