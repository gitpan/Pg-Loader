
# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .

package Pg::Loader::Log;

use v5.10;
use Data::Dumper;
use strict;
use warnings;
use base 'Exporter';
use Log::Log4perl qw( :easy );

our $VERSION = '0.11';
our @EXPORT = qw(
	      REJECTLOG   del_stack
);


my $l = get_logger('rej_log');

sub REJECTLOG {
	$l->info( $_[0] ) if $_[0];
}

sub del_stack {
 	INFO  ;
 	Log::Log4perl::NDC->remove;
}

1;
__END__
=head1 NAME

Pg::Loader::Log - Helper module for Pg::Loader

=head1 SYNOPSIS

  use Pg::Loader::Log;

=head1 DESCRIPTION

This is a helper module for pgloader.pl(1). It controls messages
for rejected entries.


=head2 EXPORT


Pg::Loader::Log - Helper module for Pg::Loader


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
   
