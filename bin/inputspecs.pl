#!/usr/bin/perl -w
#

use autodie;
use Getopt::Long;
use File::Basename;
#use Cwd 'abs_path';
#use JSON;

use English qw( -no_match_vars ) ; # Avoids regex performance penalty
use strict;
use warnings;

use Mu2eFilename;
use Mu2eDSName;

use Data::Dumper; # for debugging

use constant proto_ifdh => 'ifdh';
use constant proto_file => 'file';
use constant proto_root => 'root';

my @protocols = ( proto_ifdh, proto_file, proto_root );

#================================================================
# prepends a prefix str (first arg) to each line of text in the second arg
sub prefix_text_block($$) {
    my ($prefix, $text) = @_;
    $text =~ s/^/$prefix/mg;
    return $text;
}

#================================================================
sub help_on_protocols($) {
    my ($protolist) = @_;

    my %proto_help = (
        proto_ifdh() => "Use ifdh to pre-stage the files to the worker node.\n",
        proto_file() => "Read the files directly using their Unix pathnames.\n",
        proto_root() => "Read the files via xrootd.\n"
        );

    my $res = '';

    for my $p (@$protolist) {
        $res .= $p . '    ' . $proto_help{$p} . "\n";
    }

   return $res;
}

#================================================================
sub help_on_locations {
    my %args = ( ALLOW_LOCAL => 0, @_ );

    my $res = '';

    for my $loc (Mu2eFNBase::standard_locations()) {
        $res .= sprintf("%-16s Mu2e-standard location in PNFS\n\n", $loc);
    }
    if($args{ALLOW_LOCAL}) {
        my $loc = "/abs/dir/name";
        $res .= sprintf("%-16s Read files from the given local directory.\n\n", $loc);
    }

   return $res;
}


#================================================================
sub usage() {
    my $self = basename($0);
    my $protostr = '' . proto_file . ',' . proto_root;
    my $stdlocs = join ',', Mu2eFNBase::standard_locations();
    return <<EOF
Usage:
    $self \\
       [--default-protocol <protocol>]\\
       [--protocol <dataset>:<protocol>]\\
       [--default-location <location>]\\
       [--location <dataset>:<location>]\\
       [-h|--help]

The following set of options specifies where to find input files and
how to access them.

    --default-location <location>

      Sets the default value which can be overriden for individual
      datasets.  The possible values of <location> are

EOF
. prefix_text_block(' 'x10, help_on_locations(ALLOW_LOCAL => 1))
. <<EOF
    --location <dsname>:<location>
      Use the specified location to access files from the given dataset.

    --default-protocol  <protocol>
      Sets the default value which can be overriden for individual
      datasets.  The possible values of <protocol> are

EOF
. prefix_text_block(' 'x10, help_on_protocols([proto_file, proto_root,proto_ifdh]))
. <<EOF
    --protocol  <dsname>:<protocol>
      Use the specified protocol to access files from the given dataset.

EOF
}

#================================================================
my %opt;

GetOptions(\%opt,
           'protocol=s',
           'location=s',
           'help',
    )
    or die "\nError processing command line options.\n";

if($opt{'help'}) {
    print usage();
    exit 0;
}

print "doing something\n";

#================================================================

