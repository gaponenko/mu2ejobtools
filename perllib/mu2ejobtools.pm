# Code shared by mu2ejobtools scripts
#
# A.Gaponenko, 2023
#

package mu2ejobtools;
use parent qw(Exporter);

use strict;
use Carp;

#================================================================
sub doubleQuote {
    my @res = map {'"'.$_.'"' } @_;
    return @res if wantarray;
    croak "doubleQuote() called in a scalar content,"
        ." expect a single arg, got (@_) instead\n"
        unless scalar(@res) == 1;
    return $res[0];
}

#================================================================
our $VERSION = '1.00';

our @EXPORT   = qw(
    doubleQuote
    );

#================================================================
1;
