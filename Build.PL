use Module::Build;
my $b= Module::Build->new(
        module_name        => 'Pg::Loader',
        dist_version_from  => 'lib/Pg/Loader.pm',
        dist_author        => 'Ioannis Tambouras<ioannis@cpan.org>',
        requires           => {  perl                 => '5.010000',
                                #'FindBin'            => '1.47',
                                #'Text::Autoformat'   => '0',
                                #'Carp::Assert'       => '0.18',
                                #'File::Slurp'        => '9999.12',
                                'Log::Log4perl'       => '1.16',
                                'Config::Format::Ini' => '0.05',
                                'DBD::Pg'             => '2.8.1',
                                'Time::HiRes'         => '1.9711',
                                'List::MoreUtils'     => '0.22',
                                'Data::Dumper'        => '2.121_08',
                                'Text::CSV'           => '1.06',
                                'Getopt::Compact'     => '0.04',
                                'Quantum::Superpositions' => '2.02',
                              #},
        #recommends         => { 
				'Test::Pod'           =>  1.0,
                                'Test::Pod::Coverage' => '1.0',
                                'Test::MockDBI'       => '0.61',
                                'Test::Exception'     => '0.27',
                              },
        script_files       =>  [ 'script/pgloader.pl' ],
        license            => 'perl',
        #create_readme     => 1,
);

$b->create_build_script;
