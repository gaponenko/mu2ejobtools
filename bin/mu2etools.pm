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

use constant fclkey_randomSeed => 'services.SeedService.baseSeed';
use constant fclkey_TFileServiceFN => 'services.TFileService.fileName';
use constant fclkey_outModFMT => 'outputs.%s.fileName';

use constant filename_json => 'jobpars.json';
use constant filename_fcl => 'mu2e.fcl';
use constant filename_tarball => 'code.tar';
use constant filename_tarsetup => 'Code/setup.sh';

#================================================================
sub doubleQuote($) {
    my ($fn) = @_;
    return  '"'.$fn.'"';
}

#================================================================
our $VERSION = '1.00';

our @EXPORT      = qw(
                      doubleQuote
                      fclkey_randomSeed
                      fclkey_TFileServiceFN
                      fclkey_outModFMT
                      filename_json
                      filename_fcl
                      filename_tarball
                      filename_tarsetup
   );

#================================================================
1;
