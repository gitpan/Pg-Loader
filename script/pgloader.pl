#!/usr/bin/env  perl 

use v5.10;
use lib qw( ../blib/lib ) ;
use Pg::Loader;
use Pg::Loader::Misc;
use Pg::Loader::Query;
use Data::Dumper;
use Log::Log4perl  qw( :easy );
use strict;
use warnings;

our $VERSION = '0.01';

my $conf = fetch_options(); 
#l4p_config( $conf );


my $ini  = ini_conf    $conf->{config} ;
error_check_pgsql( $conf, $ini );

my $dh   = connect_db  $ini->{pgsql};

show_sections($conf, $ini)  unless @ARGV;

## MAIN
my @stats;

for  ( @ARGV ) {
	add_defaults( $ini, $_ )  ;
	# setup per-section logging
	l4p_config( $conf, @{$ini->{$_}}{qw( reject_data reject_log )});

        # update, or load table
        $ini->{$_}{mode} eq 'update' 
 	 	? (push @stats,    update_loader( $conf, $ini, $dh , $_)) 
 	 	: (push @stats,    copy_loader( $conf, $ini, $dh , $_))  ;
};

print_results( @stats)  if $conf->{summary};

END { $dh and $dh->disconnect }


__END__	
=head1 NAME

pgloader.pl - loads data to Postgres tables

=head1 SYNOPSIS

  pgloader.pl  -siTV  person 
  pgloader.pl  --help


=head1 DESCRIPTION

I<pgloader.pl> loads tables to a Postgres database. It is similar
to the pgloader(1) python program, written by other authors. Data
are read from the file specified in the configuration file (defaults
to pgloader.dat).

This version of pgloader exhibits the -i option which (when activated)
drops all table indexes and recreates them again after COPY.  In case of 
errors, everything rolls back to the initial state. This version also
allows the libpq 'service' database connection method.

The configuration file and command options are almost identical to the 
python pythod pgloader(1) and is meant to be a drop-in replacement. 
Configuration entries are ignored for unimplemented features.
The core functionality and many usefull features are already implemented;
read further to find what is currently available.

=head1 OPTIONS

 -q                         quiet  mode     (same as loglevel=1)
 -v                         verbose mode    (same as loglevel=3)
 -d                         debug  mode     (same as loglevel=4)
 -l,  --loglevel            set loglevel 1 to 4  . Defaults to 2
 -c,  --config              configuration file; defaults to "pgloader.conf"
 -g,  --generate            generate a sample configuration file
 -i,  --indexes             disable indexes during COPY
 -n,  --dry_run             dry_run
 -s,  --summary             show summary
 -D,  --disable_triggers    disable triggers during loading
 -T,  --truncate            truncate table before loading 
 -V,  --vacuum              vacuum analyze table after loading
 -C,  --count               number of lines to process 
      --version             show version and exit
 -F,  --from                process from this line number

=head1 CONFIGURATION FILE

 
The configuration file (default is pgloader.conf), follows the ini 
configuration format, and is divided into these sections:

=over

=item [pgslq]

This section is the only mandatory section, and defines how to 
access the database.

 base           [required]  name of the database
 host           [optional]  hostname to connect. Default is 'localhost'
 port           [optional]  port number. Default is 5432
 user           [optional]  name of login user. Default is epid of user
 pass           [optional]  user password. Not needed if using libpq defaults.
 pgsysconfdir   [optional]  dir for PGSYSCONFDIR
 service        mandatory only when pgsysconfdir ( or the enviromental 
                variable PGSYSCONFDIR ) is defined .

=item [template1]

This section defines templates. In this case, the name was arbitrary
chosen as B<template1>. The purpose of templates is to hold default
values for other table sections (defined bellow).  You may define an 
unlimited number of template sections.  The only mandatory entry 
for this section is 'template': 

 template   when defined, the template as enabled; leave it blank 
            to disable it.

 

=item [person]

This is the table section. The name B<person> was arbitrary choosen,
you can define an unlimited number of table sections.  If the name of a
table section appears on the command line (when invoking pgloader.pl) 
the corresponding table section defines how to load this table.
Try to keep the name of the section the same as the name of the table.
In a table section you can define the following parameters:

 table                [ MANDATORY ]  Tablename or schema.tablename . 
                      Defaults to section name.       

 filename             [ OPTIONAL ]  Filename with data for the table   
	              If missing, or set to 'STDIN', input data should
                      arrive from standard input.
                       
 use_template         [ OPTIONAL ]   Template to use for default values.

 field_sep            Delimiter that separates fields. The default for
                      text formats is TAB, and for csv formats is ',' 

 format               [ OPTIONAL ]   Must be either 'text' or 'csv' (without 
                      the quotes). Default is text.

 copy                 [ OPTIONAL ]   Names of columns found in data file. 
                      The names must match those in the database table.
                      Defauls to * . If you don't wish to list the names
                      in there proper order, you must append a number next
                      to their name; useful when the data file contains data
                      data in different order.
	              Example:  copy = age, last, first
                                copy = first:3, age:1, last:2

copy_columns          [ OPTIONAL ]    Names of columns to use for COPY.     
	              The char '*' means all columns specified with
                      the "copy" parameter; carefull, it does not mean
                      all columns defined for the database table, for it
                      would not make sence, much or little.  Default is '*', 
                      again, this means, same as "copy".  For this parameter,
                      names need not obey a particular order.
		      Example:  copy_columns = first, last, age
	                        copy_columns = *

update                [ OPTIONAL ]   Names of columns found in data file. 
                      By including this tag, you are switching to the UPDATE
                      mode for the purpose to change some (or all) fields
                      of an existing row. Updates are allowed only if the
                      the table contains primary keys.
	              The format and semantics are identical to "copy".

update_copy           [ OPTIONAL ]    Names of columns for the update mode. 
	              The format and semantics are identical to "copy_columns".

reject_data           [ OPTIONAL ]    Specifies the pathname for the file
                      that records rejected data. Default is STDOUT .
                      Output is enabled by the default or a more permissive 
                      logging level.

reject_log            [ OPTIONAL ]    Specifies the pathname for the file
                      that records diagnostics. Default is STDERR .
                      Output is enabled by the default or a more permissive 
                      logging level.

only_cols             [ OPTIONAL ]    Same purpose as "copy_columns", but here 
                      we use numbers (instead of names), to specify the 
                      columns. Numbers start from 1, ranges are also allowed. 
                      The char '*' means all columns, and is the default.
                      Example: only_cols = 1-2, 3, 5
			       only_cols = 3

 quotechar            [ OPTIONAL ]    Usefull only for csv formats. Default is "

 null                 [ OPTIONAL ]    String that indicates the  NULL value ; 
                      usefull only for text mode. Default is string '\NA'

skipinitialspace      [ OPTIONAL ]    Ignore leading and trailing whitespace

udc_COLUMNAME         [ OPTIONAL ]    Assign this value for all rows whose name
                      is column COLUMNAME
		      Examples: udc_title = Sir 
                                udc_age   = 99 
                                udc_race  = white

reformat              [ OPTIONAL ]    reformat values of the age column by 
                      passing it to function upper(), in the John::Util 
                      module reformat = age:John::Util::upper

copy_every            [ OPTIONAL ]    How many tuples to copy per transaction. 
                      More transactions are automatically created to 
                      insert the rest of the date, each inserting
                      upto that many tuples. Defaults is 10_000
                      TIP: set this parameter to 1 if you wish
                      to avoid the case where one bad tuple
                      cause other tuples to also fail.
datestyle             [ OPTIONAL ]   Set datestyle parameter, omit all quotes.
                      Example:  datestyle=euro
                                datestyle=us

client_encoding       [ OPTIONAL ]   Set client encoding, omit all quotes.
lc_messages           [ OPTIONAL ]   Set lc messages parameter, omit all quotes.
lc_numeric            [ OPTIONAL ]   Set lc numeric parameter, omit all quotes.
lc_monetary           [ OPTIONAL ]   Set lc monetary, omit all quotes.
lc_time               [ OPTIONAL ]   Set lc time, omit all quotes.


NOTE: Because of how the ini format is defined as a value separator,
if you need to include the , char, you must escape it with \ . For
example:
 field_sep = \,          sets field_sep to char ','

=back


=head1 SEE ALSO

http://pgfoundry.org/projects/pgloader/  hosts the official python
project. This project has nothing to do with this Perl program.


=head1 AUTHOR

Ioannis Tambouras, E<lt>ioannis@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 by Ioannis Tambouras

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.


=cut

