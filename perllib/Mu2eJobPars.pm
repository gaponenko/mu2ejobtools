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
sub njobs {
    my ($self) = @_;

    my $tbs = $self->json->{'tbs'}
    or croak "Error: njobs(): could not extract tbs"
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
sub input_datasets {
    my ($self) = @_;

    my $tbs = $self->json->{'tbs'}
    or croak "Error: njobs(): could not extract tbs"
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
sub job_primary_inputs {
    my ($self, $index) = @_;

    my $res = {};

    if(my $in = $self->{'json'}->{'tbs'}->{'inputs'}) {
        my ($k, $v) = %$in;
        my $merge = $v->[0];
        my $filelist = $v->[1];

        # skip the first $index * $merge files,
        # then take what is left, up to $merge
        my $nf = scalar(@$filelist);
        my $first = $index*$merge;
        my $last = $first + $merge - 1;
        $last = $nf - 1 if $nf - 1 < $last;

        die "primary_inputs(): invalid index $index\n"
            unless $first <= $last; # no emtpy lists

        $res->{$k} = [ @$filelist[$first .. $last] ];
    }

    return $res;
}

#================================================================
sub _my_random {
    my $h = Digest->new("SHA-256");
    foreach my $i (@_) {
        $h->add($i);
    }

    # the result should be the same on all systems
    # as long as the use of hexdigest is portable.
    my $rnd = hex substr($h->hexdigest, 0, 8);  #FIXME: increase this from 32 bits?  we may have 10^6 files...
    return $rnd;
}

#================================================================
sub job_aux_inputs {
    my ($self, $index) = @_;

    my $res = {};

    if(my $au = $self->{'json'}->{'tbs'}->{'auxin'}) {

        keys %$au; # reset the iterator
        while(my ($k, $v) = each (%$au)) {

            my $nreq = $v->[0];
            my @infiles = @{$v->[1]};

            # zero means take all files
            $nreq = scalar(@infiles) unless $nreq;

            # We want to draw nreq "random" files from the list, without
            # repetitions.
            my @sample;
            for(my $count = 0; $count < $nreq; ++$count) {

                # The "random" part has to be reproducible. Instead of relying on
                # an external random number generator, make one from our inputs.
                my $rnd = _my_random($index, @infiles);
                my $index = $rnd % scalar(@infiles);
                push @sample, $infiles[$index];
                splice @infiles, $index, 1; # drop the file we just used from inputs
            }

            $res->{$k} = [ @sample ];
        }
    }

    return $res;
}

#================================================================
# returns a hash ref { fcl_key => [ @files ] }
sub job_inputs {
    my ($self, $index) = @_;

    return { %{$self->primary_inputs($index)},
                 %{$self->aux_inputs($index)} };
}

#================================================================
# See https://mu2ewiki.fnal.gov/wiki/FileNames#sequencer
sub sequencer {
    my ($self, $index) = @_;

    my $pin = $self->primary_inputs($index);

    if(%$pin) {
        my ($k, $files) = %$pin;
        my @seqs = map { my $f = Mu2eFilename->parse($_); $f->sequencer } @$files;
        # if input file names use a consistent formatting for the sequencer
        # then string sort will do what is needed
        @seqs = sort @seqs;
        return $seqs[0];
    }
    elsif(my $evid = $self->{'json'}->{'tbs'}->{'event_id'}) {
        my $run = $evid->{'source.firstRun'}
        or die "Error: get_sequencer(): can not get source.firstRun from event_id\n";
        my $subrun = $index;
        return sprintf('%06d_%08d', $run, $subrun);
   }

    die "Error: get_sequencer(): unsupported JSON content\n";
}

#================================================================
__END__
=head1 NAME

Mu2eJobPars - a class to query information in a Mu2e job parameter file

=head1 DESCRIPTION

To create an instance of the Mu2eJobPars class use

    my $jp = Mu2eJobPars->new($parfilename);

where $parfilename is the name of a "Mu2e job parameters file"
that was previously created with the mu2ejobdef script.
Mu2eJobPars methods allow to query (but not modify) information
in the jobpar file.

These methods return information pertaining to the job pars file as a
whole:

    $jp->njobs()
    Returns the number of defined jobs, 0 means unlimited.

    $jp->input_datasets()
    Returns a list of all datasets used by all the defined jobs.  (A
    dataset gets on the list if there is a file from that dataset used
    either as primary or secondary input for any of the jobs.)

    $jp->json()
    returns the toplevel JSON object in the file

    $jp->get_tar_member($name)
    returns the content of the named file stored in jobpars.

There are also methods that return information for a single job number
$index defined by the par file:

    $jp->job_inputs($index);
    $jp->job_primary_inputs($index);
    $jp->job_aux_inputs($index);
    Return a reference to a (possibly empty) hash.  The keys of the
    hash are FCL keys for the inputs (one or zero for
    primary_inputs(), arbitrary number for aux_inputs).  The values
    are references to arrays that contain basenames of the input
    files.  job_inputs() is a union of primary_inputs() and
    aux_inputs().

=head1 AUTHOR

Andrei Gaponenko, 2023

=cut
