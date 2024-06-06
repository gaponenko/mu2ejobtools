# Code shared by mu2ejobtools scripts
#
# A.Gaponenko, 2023
#

package mu2ejobtools;
use parent qw(Exporter);

use strict;
use warnings;
use Carp;

# top level key name in json files read and written by mu2ejobsub and
# utilities in this package
use constant json_key_jobset => 'jobs';
use constant json_key_inspec => 'inspec';

#================================================================
sub doubleQuote {
    my @res = map {'"'.$_.'"' } @_;
    return @res if wantarray;
    croak "doubleQuote() called in a scalar content,"
        ." expect a single arg, got (@_) instead\n"
        unless scalar(@res) == 1;
    return $res[0];
}

##================================================================
## the args are numFilePerJob and the filelist ref
sub calculate_njobs {
    my ($merge, $files) = @_;

    my $nf = scalar(@$files);

    use integer;
    my $njobs = $nf/$merge + (($nf % $merge) ? 1 : 0);

    return $njobs;
}

#================================================================
our $VERSION = '1.00';

our @EXPORT   = qw(
    json_key_jobset
    json_key_inspec
    doubleQuote
    calculate_njobs
    );

#================================================================
1;
