#!/usr/bin/perl -w
#
# Code shared by different mu2ejobtools scripts
#
# A.Gaponenko, 2023
#

package mu2ejobtools;
use parent qw(Exporter);

use strict;
use Carp;
use Cwd 'abs_path';
use Archive::Tar;
use POSIX qw(ceil);
use English qw( -no_match_vars ) ; # Avoids regex performance penalty

use Data::Dumper; # for debugging

#================================================================
# Define strings that are used in multiple places

use constant fclkey_randomSeed => 'services.SeedService.baseSeed';
use constant fclkey_TFileServiceFN => 'services.TFileService.fileName';
use constant fclkey_outModFMT => 'outputs.%s.fileName';

use constant filename_json => 'jobpars.json';
use constant filename_fcl => 'mu2e.fcl';
use constant filename_tarball => 'code.tar';
use constant filename_tarsetup => 'Code/setup.sh';

use constant proto_file => 'file';
use constant proto_root => 'root';

use constant location_local => 'local'; # others come from Mu2eFNBase.pm

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
sub get_tar_member($$) {
    my ($archive, $membername) = @_;
    my $tar = Archive::Tar->new();
    my $res;
    if ( $tar->read($archive, 1,
                    { filter => qr{^$membername$},
                      limit => 1 # avoid reading the possible code tarball
                    } )
        )
    {
        $res = $tar->get_content($membername);
    }
    return $res;
}

#================================================================
sub get_njobs($) {
    my ($js) = @_;
    my $tbs = $js->{'tbs'}
    or croak "Error: get_njobs(): could not extract tbs from the json\n";

    my $njobs = 0;
    if(my $in = $tbs->{'inputs'}) {
        my ($k, $v) = %$in;
        my $merge = $v->[0];
        my $files = $v->[1];
        my $nf = scalar(@$files);
        use integer;
        $njobs = $nf/$merge + (($nf % $merge) ? 1 : 0);
    }
    return $njobs;
}

#================================================================
our $VERSION = '1.00';

our @EXPORT      = qw(
                      fclkey_randomSeed
                      fclkey_TFileServiceFN
                      fclkey_outModFMT
                      filename_json
                      filename_fcl
                      filename_tarball
                      filename_tarsetup
                      proto_file
                      proto_root
                      location_local

                      doubleQuote
                      get_tar_member
                      get_njobs
   );

#================================================================
1;
