#!/usr/bin/perl -w
#
# Interfaces to work with Mu2e job definition files.
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

use Mu2eFilename;

#================================================================
# Define strings that are used in multiple places

use constant filename_json => 'jobpars.json';
use constant filename_fcl => 'mu2e.fcl';


#FIXME BEGIN: remove this
use constant proto_file => 'file';
use constant proto_root => 'root';

use constant location_local => 'local'; # others come from Mu2eFNBase.pm
#FIXME END

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
# arg is the top-level json object
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
# arg is the top-level json object
sub get_datasets($) {
    my ($js) = @_;
    my $tbs = $js->{'tbs'}
    or croak "Error: get_njobs(): could not extract tbs from the json\n";

    my %datasets; # use hash to remove the dups

    if(my $in = $tbs->{'inputs'}) {
        my ($k, $v) = %$in;
        my $filelist = $v->[1];
        foreach my $i (@$filelist) {
            my $f = Mu2eFilename->parse($i);
            ++$datasets{$f->dataset->dsname};
        }
    }

    if(my $au = $tbs->{'auxin'}) {
        keys %$au; # reset the iterator
        while(my ($k, $v) = each(%$au)) {
            my $filelist = $v->[1];
            foreach my $i (@$filelist) {
                my $f = Mu2eFilename->parse($i);
                ++$datasets{$f->dataset->dsname};
            }
        }
    }

    print "Got datasets ",Dumper(%datasets),"\n";

    return keys %datasets;
}

#================================================================
our $VERSION = '1.00';

our @EXPORT      = qw(
                      get_tar_member
                      get_njobs
                      get_datasets
   );

our @EXPORT_OK   = qw(
                      filename_json
                      filename_fcl

                      proto_file
                      proto_root
                      location_local

                      doubleQuote
   );

#================================================================
1;
