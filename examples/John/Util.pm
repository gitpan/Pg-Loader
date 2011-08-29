package John::Util;

sub jupper {
	uc shift;
}
sub changed {
	ucfirst shift;
}
# nafc,1.87,Aug 20,0.72

sub addyear {
	my $got =  shift ; 
	return  unless $got =~ /^[A-Za-z]{3}\s*\d{1,2}$/o;
	$got = '2008-'. $got;
	$got =~ s/\ /-/x ;
	$got =~ s/\ //x ;
	$got;
}
1;
