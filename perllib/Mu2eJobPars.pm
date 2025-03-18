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
use mu2ejobtools;

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
sub jobname {
    my ($self) = @_;
    my $jobname = $self->{'json'}->{'jobname'};
    croak "Error: no jobname in " . $self->{parfilename} . "\n"
        unless defined $jobname;

    return $jobname;
}

#================================================================
sub setup {
    my ($self) = @_;
    my $res = $self->{'json'}->{'setup'};
    croak "Error: no setup in " . $self->{parfilename} . "\n"
        unless defined $res;

    return $res;
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
        $njobs = calculate_njobs($merge, $files);
    }
    elsif(my $sin = $tbs->{'samplinginput'}) {
        # consistency checks are front-loaded and done
        # in mu2ejobdef, do not repeat them here

        keys %$sin; # reset the iterator
        my ($k, $v) = each(%$sin);
        my $merge = $v->[0];
        my $filelist = $v->[1];
        $njobs = calculate_njobs($merge, $filelist);
    }

    return $njobs;
}

#================================================================
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

    if(my $su = $tbs->{'samplinginput'}) {
        keys %$su; # reset the iterator
        while(my ($k, $v) = each(%$su)) {
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
sub output_datasets {
    my ($self) = @_;

    my $tbs = $self->json->{'tbs'}
    or croak "Error: njobs(): could not extract tbs"
        . " from the json for file ".$self->parfilename."\n";

    my @dslist;
    if(my $out = $tbs->{'outfiles'}) {
        push @dslist,
            map { my $f = Mu2eFilename->parse($_);
                  $f->dataset->dsname; }
            values %$out;
    }
    return @dslist;
}

#================================================================
sub codesize {
    my ($self) = @_;

    my $sz = 0;

    if(my $codename = $self->{'json'}->{'code'}) {
        my $tar = Archive::Tar->new();

        # We do not need to store the content of the code tarball in
        # memory.  Use the md5 option here to drastically reduce
        # memory consumption.
        $tar->read($self->{'parfilename'}, 1, {md5 => 1, filter => qr{^$codename$} } );
        my @props = ('size');
        my @list = $tar->list_files(\@props);
        die "Error extracting the size of arvhive member '$codename' in '".$self->{'parfilename'}."'\n"
            unless @list;

        $sz = $list[0]->{'size'};
    }

    return $sz;
}

#================================================================
sub extract_code {
    my ($self) = @_;

    if(my $code = $self->{'json'}->{'code'}) {
        my $pfname = $self->{'parfilename'};
        open(my $in, '-|',
             'tar', '--extract', '--to-stdout', "--file=$pfname", $code)
            or croak "Error running tar on $pfname\n";

        open(my $out, '|-',
             'tar', '--extract', '--bzip2', "--file=-")
            or die "Error running code expansion tar\n";

        my $BUFSIZE = 64 * (2**20);
        my $buf;
        while(read($in, $buf, $BUFSIZE)) {
            print $out $buf
                or die "Error extracting the code: $!\n";
        }

        close($in) or croak "Error closing input tar pipe: $!\n";
        close($out) or croak "Error closing output tar pipe: $!\n";
    }
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

        die "job_primary_inputs(): invalid index $index\n"
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
sub job_sampling_inputs {
    my ($self, $index) = @_;

    my $res = {};

    if(my $au = $self->{'json'}->{'tbs'}->{'samplinginput'}) {

        keys %$au; # reset the iterator
        while(my ($k, $v) = each (%$au)) {

            my $nreq = $v->[0];
            my $filelist = $v->[1];

            # zero means take all files
            $nreq = scalar(@$filelist) unless $nreq;

            # SamplingInput files are used like sequentially, similar
            # to primary but not aux inputs.
            #
            # skip the first $index * $nreq files,
            # then take what is left, up to $nreq
            my $nf = scalar(@$filelist);
            my $first = $index*$nreq;
            my $last = $first + $nreq - 1;
            $last = $nf - 1 if $nf - 1 < $last;

            die "job_sampling_inputs(): invalid index $index\n"
                unless $first <= $last; # no emtpy lists

            $res->{$k} = [ @$filelist[$first .. $last] ];
        }
    }

    return $res;
}

#================================================================
sub job_inputs {
    my ($self, $index) = @_;

    return { %{$self->job_primary_inputs($index)},
             %{$self->job_aux_inputs($index)},
             %{$self->job_sampling_inputs($index)},
    };
}

#================================================================
# See https://mu2ewiki.fnal.gov/wiki/FileNames#sequencer
sub sequencer {
    my ($self, $index) = @_;

    my $pin = $self->job_primary_inputs($index);

    if(%$pin) {
        my ($k, $files) = %$pin;
        my @seqs = map { my $f = Mu2eFilename->parse($_); $f->sequencer } @$files;
        # if input file names use a consistent formatting for the sequencer
        # then string sort will do what is needed
        @seqs = sort @seqs;
        return $seqs[0];
    }
    elsif(my $evid = $self->{'json'}->{'tbs'}->{'event_id'}) {
        my $run = $evid->{'source.firstRun'} // $evid->{'source.run'}
        or die "Error: get_sequencer(): can not get source.firstRun from event_id\n";
        my $subrun = $index;
        return sprintf('%06d_%08d', $run, $subrun);
   }

    die "Error: get_sequencer(): unsupported JSON content\n";
}

#================================================================
sub index_from_sequencer {
    my ($self, $seq) = @_;

    my $nj = $self->njobs();
    if($nj) {
        # A job set based on an input file list.  The sequencers are
        # not predictable.  We have to do an exhaustive search in the
        # finite set.
        my $ind;
      LOOP: for(my $i=0; $i < $nj; ++$i) {
          my $testseq = $self->sequencer($i);
          if($testseq eq $seq) {
              $ind = $i;
              last LOOP;
          }
        }
        croak "Outputs with the requested sequencer \"$seq\" are not produced by this jobset. "
            unless defined $ind;

        return $ind;

    }
    else {
        # An infinite job set.  Try to parse the sequencer and find
        # out the index.  The format is defined by the sequencer()
        # function in this file.
        my $ind = $seq;
        $ind =~ s/^\d+_//;
        croak "Unexpected format of the sequencer \"$seq\""
            unless $ind =~ /^\d+$/;

        $ind = 0 + $ind; # make it numeric

        # verify
        my $testseq = $self->sequencer($ind);

        croak "Outputs with the requested sequencer \"$seq\" are not generated by this jobset. "
            unless $testseq eq $seq;

        return $ind;
    }
}

#================================================================
sub index_from_source_file {
    my ($self, $srcfn) = @_;

    if(my $in = $self->{'json'}->{'tbs'}->{'inputs'}) {
        my ($k, $v) = %$in;
        my $merge = $v->[0];
        my $filelist = $v->[1];

        my $fileindex;
        for(my $i=0; $i < scalar @$filelist; ++$i) {
            if($filelist->[$i] eq $srcfn) {
                $fileindex = $i;
                last;
            }
        }

        if(defined $fileindex) {
            use integer;
            return $fileindex / $merge;
        }
    }

    croak "This jobset does not use \"$srcfn\" as a primary input file\n";
}

#================================================================
sub job_outputs {
    my ($self, $index) = @_;

    my $res = {};

    if(my $out = $self->{'json'}->{'tbs'}->{'outfiles'}) {

        my $seq = $self->sequencer($index);

        keys %$out; # reset the iterator, just in case
        while(my ($k, $v) = each(%$out)) {
            my $f = Mu2eFilename->parse($v);
            $f->sequencer($seq);
            $res->{$k} = $f->basename();
        }
    }

    return $res;
}

#================================================================
sub job_event_settings {
    my ($self, $index) = @_;

    my $res = {};

    if(my $evid = $self->{'json'}->{'tbs'}->{'event_id'}){
        foreach my $k (keys %$evid) {
            $res->{$k} = $evid->{$k};
        }

        my $srk = $self->{'json'}->{'tbs'}->{'subrunkey'};
        if(defined $srk) {
            # New format jobdef files have subrunkey defined, see
            # if we need to set subrun for this job
           if($srk ne '') {
                $res->{$srk} = $index;
            }
        }
        else {
            # We are working with an old format jobdef.
            # Since $evid is defined, we have to set
            $res->{'source.firstSubRun'} = $index;
        }
    }

    return $res;
}

#================================================================
sub job_seed {
    my ($self, $index) = @_;

    my $res = {};

    my $key = $self->{'json'}->{'tbs'}->{'seed'};
    if(defined $key) {
        $res->{$key} = 1 + $index;
    }

    return $res;
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

These methods return information pertaining to the job pars file,
not to individual jobs

    $jp->jobname()
    Returns the name of the job set.

    $jp->njobs()
    Returns the number of defined jobs, 0 means unlimited.

    $jp->input_datasets()
    Returns a list of all datasets used by the job set.  A dataset
    gets on the list if there is a file from that dataset used either
    as primary or secondary input for any of the jobs.

    $jp->output_datasets()
    Returns a list of all datasets created by the job set.

    $jp->json()
    Returns the toplevel JSON object in the file

    $jp->codesize()
    Returns the size, in bytes, of the embedded code tarball.

    $jp->get_tar_member($name)
    Returns the content of the named file stored in jobpars.

    $jp->extract_code()
    This call expands the embedded code tarball, if any, in the
    current directory.  The return value is not meaningful.

    $jp->setup()
    Returns the name of the Offline setup file.

There are also methods that return information for a single job number
$index defined by the par file.  Any of the returned hashes may be empty,
meaning this group of FCL setting is not needed.

    $jp->job_inputs($index);

    $jp->job_primary_inputs($index);
    $jp->job_aux_inputs($index);
    $jp->job_sampling_inputs($index);

    Returns a reference to a hash.  The keys of the hash are FCL keys
    for the inputs (one or zero for primary_inputs(), arbitrary number
    for aux_inputs).  The values are references to arrays that contain
    basenames of the input files.  job_inputs() is a union of primary,
    aux, and sampling inputs.

    $jp->sequencer($index)
    The sequencer, see https://mu2ewiki.fnal.gov/wiki/FileNames#sequencer

    $jp->index_from_sequencer($seq)
    Returns the index of the job in this jobset that corresponds to the
    sequencer.  Croaks if the sequencer is not produced by the job set.

    $jp->index_from_source_file($srcfn)
    Returns the index of the job in this jobset that uses the given
    filename as a primary input. Croaks if none of the jobs use that file.

    $jp->job_outputs($index);
    Returns a reference to a hash mapping FCL keys to output file names.

    $jp->job_event_settings($index)
    Returns a reference to a hash.  The keys are FCL keys and values
    are settings for the EmptyEvent source in the given job.

    $jp->job_seed($index)
    Returns a reference to a hash with zero or one entry.  A non-empty
    result has FCL key and seed value for the job.

=head1 AUTHOR

Andrei Gaponenko, 2023, 2024

=cut
