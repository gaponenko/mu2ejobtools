#!/usr/bin/perl -w
#
# Code shared by different mu2etools scripts
#
# A.Gaponenko, 2023
#

package mu2etools;
use parent qw(Exporter);

use strict;
use Cwd 'abs_path';
use POSIX qw(ceil);
use English qw( -no_match_vars ) ; # Avoids regex performance penalty

#================================================================
# Define strings that are used in multiple places

use constant jobtype_artgen => 'artgen';
use constant jobtype_fileinputs => 'fileinput';

use constant fclkey_randomSeed => 'services.SeedService.baseSeed';
use constant fclkey_TFileServiceFN => 'services.TFileService.fileName';
use constant fclkey_outModFMT => 'outputs.%s.fileName';

use constant tarball_setup => 'Code/setup.sh';

#================================================================
sub doubleQuote($) {
    my ($fn) = @_;
    return  '"'.$fn.'"';
}

#================================================================
our $VERSION = '1.00';

our @EXPORT      = qw(
                      doubleQuote
                      jobtype_artgen
                      jobtype_fileinputs
                      fclkey_randomSeed
                      fclkey_TFileServiceFN
                      fclkey_outModFMT
                      tarball_setup
    );

#================================================================
1;
