# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .

package Pg::Loader::Query;

use v5.10;
use DBI;
use Data::Dumper;
use strict;
use warnings;
use base 'Exporter';
use Config::Format::Ini;
use Log::Log4perl qw( :easy );
use Pg::Loader::Columns;
use Data::Dumper;
use Text::CSV;

our $VERSION = '0.02';

our @EXPORT = qw(
	connect_db	column_names_str   get_columns_names
	disable_indexes enable_indexes
);

sub connect_db {
        my $pgsql   =  shift                                       ;
        my ($port, $host, $base) = @{$pgsql}{'port','host','base'} ;
        $port    //=  5432                                         ;
        $host    //=  'localhost'                                  ;
        $base    ||   usage()                                      ;
        my ($user, $pass) = @{$pgsql}{'user','pass'}               ;
        my $dsn    =  "dbi:Pg:dbname=$base;host=$host;port=$port"  ;
	$dsn .=';options=--client_min_messages=WARNING'            ;
        $ENV{ PGSYSCONFDIR } //= $pgsql->{pgsysconfdir} //''       ;
	if ( -f "$ENV{ PGSYSCONFDIR }/pg_service.conf") {
		DEBUG( "Using PGSYSCONFIGDIR ")            ;	
                $dsn = "dbi:Pg:service=$pgsql->{service}"  ;
		$user = $pass = ''                         ;
	}
        my $att  = { AutoCommit => 0 , pg_server_prepare => 1,
                     PrintError => 0 , Profile           => 0,
		   };
        DBI->connect( $dsn, $user // getlogin,$pass,$att) or die $DBI::errstr;
}

sub disable_indexes {
        my ( $dh, $schema, $table) = ($_[0], schema_name( $_[1]  ));
	(my $st = $dh->prepare(<<""))->execute() ;
		SELECT  indexrelid::regclass::text  AS name, 
			indisprimary                AS pk,
		        pg_get_indexdef(indexrelid) AS def
		FROM  pg_index  I
		 join pg_class  C    ON ( C.oid = I.indrelid )
		 join pg_namespace N ON ( N.oid = C.relnamespace )
		WHERE relname      = @{[ $dh->quote($table) ]}
		 and  nspname      = @{[ $dh->quote($schema) ]}

	my  @definitions;
	while ( my $idx = $st->fetchrow_hashref  ) {
		my  $sql =  $idx->{pk}
                       ? "ALTER table $table drop constraint $idx->{name}"
                       : "DROP INDEX $idx->{name}"                          ;
		DEBUG( "\t\t$sql" )                                         ;
		$dh->do( $sql )  and   INFO( "\t\tDisabled $idx->{name}")   ; 
	 	push @definitions, 
                   { name =>$idx->{name},def =>$idx->{def}, pk=>$idx->{pk} };
	}
	\@definitions;
}
sub enable_indexes {
        my ( $dh, $schema, $table) = ($_[0], schema_name($_[1]));
	my @defs = @{$_[2]};
	for (@defs) { 
		my ($col) = $_->{def} =~ / (\( [,\w\s]+? \)) $/xo         ;
		$col    //= '';
		my $sql = $_->{pk} ? "ALTER TABLE $table add PRIMARY KEY $col"
				   : $_->{def};
		DEBUG( "\t\t$sql" )                                       ;
		$dh->do( $sql) and INFO( "\t\tCreated index $_->{name}" ) ;
	}
}

sub schema_name {
	my ($canonical, $search) = @_ ;
	my ($schema, $table) = split /\./, $canonical, 2 ;
	unless ($table ) {
		$table  = $schema;
		$schema = $search || 'public'; 
        }
	( $schema, $table );
}

sub _get_columns_hash {
	# map column_name -> ordinal_position
}

sub get_columns_names {
        # return ordered list of culumn names
        my ( $dh, $schema, $table) = ($_[0], schema_name( $_[1]  ));
        (my $st =  $dh->prepare(<<""))->execute( $table, $schema) ;
                select column_name, ordinal_position
                from information_schema.columns
                where table_name = ?
                and table_schema = ?
                order by 2;

        my $h = $st->fetchall_arrayref;
        map { ${$_}[0] }   @$h ;
}


sub all_column_names_str {
        my ( $dh, $table) = @_ ;
	my @names = get_columns_names ($dh, $table);
	'('. join( ', ', @names) . ')' ;
}

sub column_names_str {
        my ( $dh, $table, $s) = @_ ;
	#if ($s->{copy_columns} ) {
	if ($s->{opy_columns} ) {
		my @cols = @{$s}{copy_columns} ;
		return '('. join( ', ', @cols) . ')' ;

		#while ( my( $k,$v) = each %$s) {
			#next unless $k =~ /^udc_/;
			#my (undef,$name) = split /udc_/i, $k ;
	                #next unless  grep {$name} @cols  ;
			#say $name;
		#}
		print Dumper @cols ; exit;
	}elsif ($s->{only_cols}){
		my @cols   = get_columns_names($dh, $table);
		my $r      = range2list( '1-2' );  #TODO: is hardcoded
		{ $[=1; no warnings; @cols =  eval '@cols['.$r.']' }
		return '('. join( ', ', @cols) . ')' ;
	}else{
		my @cols = get_columns_names ($dh, $table);
		return '('. join( ', ', @cols) . ')' ;
	}
}


1;
__END__

=head1 NAME

Pg::Loader::Query - Helper module for Pg::Loader

=head1 SYNOPSIS

  use Pg::Loader::Query;

=head1 DESCRIPTION

This is a helper module for pgloader.pl(1), which loads tables to
a Postgres database. It is similar in function to the pgloader(1)
python program (written by other authors).


=head2 EXPORT


Pg::Loader::Query - Helper module for Pg::Loader


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
   
