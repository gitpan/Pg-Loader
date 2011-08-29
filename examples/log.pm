package Log;
use Data::Dumper;
use Log::Log4perl ':easy';
use Log::Log4perl::Layout;
use Log::Log4perl::Level;

use base 'Exporter';
use v5.10 ;
our @EXPORT = qw( rejected main_logger file_logger );

sub main_logger {
	my $main     = get_logger('main');
	my $layout   = Log::Log4perl::Layout::PatternLayout->new( '%x' ); 
	my $appender = Log::Log4perl::Appender->new( 
                                             'Log::Log4perl::Appender::File',
                                              mode      => 'append',
                                              name      => 'stdio',
                                              filename  => '/dev/stdout');
           # config logger
           $appender->layout( $layout );
           $main->add_appender( $appender);
           $main->level( $INFO );
}

sub  file_logger {
	my $file     = shift || return ;
	my $l        = get_logger('jdsu');
	my $layout   = Log::Log4perl::Layout::PatternLayout->new( '%x%n' ); 
	my $appender = new Log::Log4perl::Appender
                                             'Log::Log4perl::Appender::File',
                                              mode      => 'clobber',
                                              name      => 'filelog',
                                              recreate  =>  0,
                                              filename  =>  $file ,
                                              recreate_signal =>'USR1';
                                                
           # config logger
           $appender->layout( $layout );
           $l->add_appender( $appender);
           $l->level( $INFO );
}

1;

sub rejected {
}
__END__
