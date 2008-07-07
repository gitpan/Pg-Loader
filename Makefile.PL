use v5.10;
use ExtUtils::MakeMaker;


WriteMakefile(
    PL_FILES          => {},
    NAME              => 'Pg::Loader',
    VERSION_FROM      => 'lib/Pg/Loader.pm',
    PREREQ_PM         => { 
	                   #'FindBin'            => '1.47',
	                   #'Carp::Assert'        => '0.18',
	                   #'File::Slurp'         => '9999.12',
	                   'Log::Log4perl'       => '1.16',
	                   'DBD::Pg'             => '2.8.1',
                           'Time::HiRes'         => '1.9711',
			   'List::MoreUtils'     => '0.22',
			   'Config::Format::Ini' => '0.05',
	                   'Data::Dumper'        => '2.121_08',
	                   'Text::CSV'           => '1.06',
	                   'Getopt::Compact'     => '0.04',
                           'Test::Pod::Coverage' => '1.0',
                           'Test::Pod'           => '1.0',
                           'Test::MockDBI'       => '0.61',
                           'Test::Exception'     => '0.27',
                           'Quantum::Superpositions' => '2.02',
                         },
    EXE_FILES         =>  [ 'script/pgloader.pl'],
    ABSTRACT_FROM     => 'lib/Pg/Loader.pm',
    AUTHOR            => 'Ioannis Tambouras <ioannis@cpan.org>',
);

#print  "\nIt takes 2 seconds. This time read the README file!\n\n";
