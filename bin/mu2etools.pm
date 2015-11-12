#!/usr/bin/perl -w
#
# Code shared by the different scripts in this package
#
# A.Gaponenko, 2012, 2015
#

package mu2etools;

use strict;
use Cwd 'abs_path';
use File::Basename;
use Digest;

use Class::Struct Mu2eFileName =>
    [tier=>'$', owner=>'$', description=>'$', configuration=>'$', sequencer=>'$', extension=>'$' ];

our $dataroot = "/pnfs/mu2e/persistent/projects/production/data";

#================================================================
sub parseMu2eFileName($) {
    my ($fn) = @_;

    my ($tier, $owner, $description, $conf, $seq, $ext, $extra) = split(/\./, basename($fn));

    die "Error parsing Mu2e file name $fn: too many fields\n" if defined $extra;
    die "Error parsing Mu2e file name $fn: too few fields\n" if not defined $ext;

    return Mu2eFileName->new(
        tier=>$tier,
        owner=>$owner,
        description=>$description,
        configuration=>$conf,
        sequencer=>$seq,
        extension=>$ext,
        );
}

#================================================================
sub makeMu2eDatasetName($) {
    my ($fields) = @_;

    return join('.',
                ($fields->tier,
                 $fields->owner,
                 $fields->description,
                 $fields->configuration,
                 ## $fields->sequencer,
                 $fields->extension)
        );
}

#================================================================
sub stdPathName($) {
    my ($filename) = @_;

    die "stdPathName(): arg should be a base name of a file without a path.  Got: $filename\n"
        if $filename =~ m|/|;

    # the "spreader" directory names are based on SHA256
    my $dig = Digest->new('SHA-256');
    $dig->add($filename);
    my $hash = $dig->hexdigest;
    my @hh = split //, $hash, 7;

    my $fn = parseMu2eFileName($filename);

    my $stdpath = $dataroot
        . '/' . $fn->tier
        . '/' . $fn->owner
        . '/' . $fn->description
        . '/' . $fn->configuration
        . '/' . $hh[0] . $hh[1]
        . '/' . $hh[2] . $hh[3]
        . '/' . $hh[4] . $hh[5]
        . '/' . $filename
        ;

    return $stdpath;
}

#================================================================
BEGIN {
    use Exporter   ();
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);

    # set the version for version checking
    $VERSION = '1.00';

    @ISA         = qw(Exporter);
    @EXPORT      = qw();

    # your exported package globals go here,
    # as well as any optionally exported functions
    @EXPORT_OK   = qw( $dataroot
                       &parseMu2eFileName
                       &makeMu2eDatasetName
                       &stdPathName
    );
}
our @EXPORT_OK;
use vars @EXPORT_OK;

#================================================================
1;
