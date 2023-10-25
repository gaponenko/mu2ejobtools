# run perldoc on this file or see POD at the end

use strict;
use warnings;

package Mu2eJobPars;
use Exporter qw ( import );

use Carp;
use Archive::Tar;
use Digest::MD5; # Archive::Tar should have requested this itself...
use JSON;

use Mu2eFilename;

#================================================================
# Define strings that are used in multiple places

use constant filename_json => 'jobpars.json';
use constant filename_fcl => 'mu2e.fcl';

our @EXPORT = qw(
    filename_json
    filename_fcl
    );

#================================================================
sub new {
    my ($class, $parfile) = @_;

    croak "Mu2eJobPars constructor should not be called on an existing instance"
        if ref $class;

    my $self = bless {
        parfilename => $parfile,
    }, $class;

    # check that the parfile arg is not completely wrong
    my $jsstr = $self->get_tar_member(filename_json)
        or croak "Mu2eJobPars->new: can not extract ". filename_json ." from $parfile.\n";

    $self->{json} = JSON->new->decode($jsstr);

    return $self;
}


#================================================================
sub get_tar_member {
    my ($self, $membername) = @_;

    my $tar = Archive::Tar->new();
    my $res;
    if ( $tar->read($self->{'parfilename'}, 1,
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
sub json {
    my ($self) = @_;
    return $self->{'json'};
}

#================================================================
sub get_njobs {
    my ($self) = @_;

    my $tbs = $self->json->{'tbs'}
    or croak "Error: get_njobs(): could not extract tbs"
        . " from the json for file ".$self->parfilename."\n";

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
sub get_datasets {
    my ($self) = @_;

    my $tbs = $self->json->{'tbs'}
    or croak "Error: get_njobs(): could not extract tbs"
        . " from the json for file ".$self->parfilename."\n";

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

    return keys %datasets;
}

#================================================================
__END__
=head1 NAME

Mu2eJobPars - a class to query information in a Mu2e job parameter file

=head1 DESCRIPTION

The Mu2eJobPars class....

=head1 AUTHOR

Andrei Gaponenko, 2023

=cut
