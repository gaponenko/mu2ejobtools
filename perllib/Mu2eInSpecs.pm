# run perldoc on this file or see POD at the end

use strict;
use warnings;

package Mu2eInSpecs;
use Exporter qw ( import );

use Mu2eFNBase;
use Mu2eFilename;

use Carp;
use Data::Dumper; # for debugging

use constant proto_file => 'file';
use constant proto_ifdh => 'ifdh';
use constant proto_root => 'root';
my @all_protocols = ( proto_file, proto_ifdh, proto_root );

use constant location_local => 'dir'; # others come from Mu2eFNBase.pm

our @EXPORT = qw(
    proto_file
    proto_ifdh
    proto_root
    location_local
    );

#================================================================
sub new {
    my $class = shift;

    croak "Mu2eInSpecs constructor should not be called on an existing instance"
        if ref $class;

    my $self = bless {
        dslist => [],
        protocols => [ @all_protocols ],
        dsproto => {}, # individual ds to protocol settings
        locations => [ @all_protocols ],
        dsloc => {}, # individual ds to location settings
    }, $class;

    return $self;
}

#================================================================
sub disable_protocol {
    my $self = shift;

    foreach my $p (@_) {
        croak "Error: $p is not a known protocol"
            unless scalar grep { $_ eq $p } @all_protocols;

        $self->{protocols} = [ grep { $_ ne $p } @{$self->{protocols}} ];
    }
}

#================================================================
# returns protocol to be used for the given dataset
sub protocol {
    my ($self, $dsname) = @_;  # one at a time, not a dslist

    my $pmap = $self->{dsproto};

    my $proto = $pmap->{$dsname} // $self->{default_proto};

    croak "Mu2eInSpecs::protocol(): no information for dataset \"$dsname\" and no default"
        unless defined $proto;

    return $proto;
}

#================================================================
# returns location for files in a given dataset
sub location {
    my ($self, $dsname) = @_;  # one at a time, not a dslist

    my $lmap = $self->{dsloc};

    my $loc = $lmap->{$dsname} // $self->{default_location};

    croak "Mu2eInSpecs::locations(): no information for dataset \"$dsname\" and no default"
        unless defined $loc;

    return $loc;
}

#================================================================
# returns the absolute path name for a Mu2e file using its location
sub abspathname {
    my ($self, $basename) = @_;
    my $fn = Mu2eFilename->parse($basename);
    my $ds = $fn->dataset->dsname;
    my $loc = $self->location($ds);
    my ($std, $dir) = split /:/, $loc, 2;

    if($std eq location_local) {
        return $dir . '/' . $basename;
    }
    else {
        return $fn->abspathname($loc);
    }
}

#================================================================
# prepends a prefix str (first arg) to each line of text in the second arg
sub _prefix_text_block($$) {
    my ($prefix, $text) = @_;
    $text =~ s/^/$prefix/mg;
    return $text;
}

sub _help_on_protocols {
    my ($self) = @_;

    my %proto_help = (
        proto_ifdh() => "Use ifdh to pre-stage the files to the worker node.\n",
        proto_file() => "Read the files directly using their Unix pathnames.\n",
        proto_root() => "Read the files via xrootd.\n"
        );

    my $res = '';

    for my $p (@{$self->{protocols}}) {
        $res .= $p . '    ' . $proto_help{$p} . "\n";
    }

   return $res;
}

#================================================================
sub option_defs {
    return
        (
         'default-protocol=s',
         'protocol=s@',
         'default-location=s',
         'location=s@',
        );
}

sub help_opts {
    my ($self, $prefix) = @_;
    $prefix = '' unless defined $prefix;

    return _prefix_text_block $prefix, <<EOF
[--default-protocol <protocol>]\\
[--protocol <dataset>:<protocol>]\\
[--default-location <location>]\\
[--location <dataset>:<location>]\\
EOF
        ;
}

sub help_explanation {
    my ($self, $prefix) = @_;
    $prefix = '' unless defined $prefix;

    my $lloc = location_local;

    return _prefix_text_block $prefix, <<EOF
--default-protocol  <protocol>
  Sets the default file access protocol that can be overriden for individual
  datasets.  The possible values of <protocol> are

EOF
. _prefix_text_block(' 'x8, $self->_help_on_protocols())
. <<EOF
--protocol  <dsname>:<protocol>
  Use the specified protocol to access files from the given dataset.

--default-location <location>
  Sets the default location that can be overriden for individual
  datasets.  The possible values of <location> are the Mu2e-standard

EOF
        . _prefix_text_block(' 'x8, join("\n", Mu2eFNBase::standard_locations()))
. <<EOF


  or an absolute path to a directory where all dataset files have been placed,
  prefixed with the $lloc: literal string:

        $lloc:</path/to/files>

--location <dsname>:<location>
  The files for the given dataset should be taken from this location.

EOF
        ;
}

#================================================================
sub _validate_location {
    my $loc = shift;

    if(grep { $_ eq $loc } Mu2eFNBase::standard_locations()) {
        return 1;
    }

    my ($prefix, $dir) = split /:/, $loc, 2;
    return 0 unless $prefix eq location_local;
    return 0 unless $dir =~ m|^/|;
    return 1;
}

#================================================================
# This should be called BEFORE parse_useropts()
sub _define_datasets {
    my $self = shift;
    $self->{dslist} = [ @_ ];
}

#================================================================
# Make sure the list of datasets is defined before calling this
sub _parse_useropts {
    my $self = shift;
    my %opt = @_;

    #----------------------------------------------------------------
    my $dfp = $opt{'default-protocol'};
    if(defined $dfp) {
        croak "Error: --default-protocol option: \"$dfp\" is not a valid protocol"
            unless 0 + grep { $_ eq $dfp } @{$self->{protocols}};
        $self->{default_proto} = $dfp;
    }

    my $dsp = $opt{'protocol'};
    if(defined $dsp) {
        foreach my $i (@$dsp) {
            # This uses the fact that Mu2e dataset names can not contain columns
            my ($ds, $proto) = split /:/, $i, 2;

            croak "Errror: the --protocol option \"$i\" does not contain a ':'"
                unless defined $proto;

            croak "Error: --protocol option: \"$proto\" is not a valid protocol"
                unless 0 + grep { $_ eq $proto } @{$self->{protocols}};

            croak "Error: --protocol option: \"$ds\" is not on the dataset list.\n"
                ."Known datasets are:\n" . join("\n", @{$self->{dslist}}) . "\n"
                unless 0 + grep { $_ eq $ds } @{$self->{dslist}};

            $self->{dsproto}->{$ds} = $proto;
        }
    }

    # Check that that we have a complete set of protocol information for the listed datasets
    if(not defined $self->{default_proto}) {
        foreach my $ds (@{$self->{dslist}}) {
            croak "Error: protocol for dataset \"$ds\" is not set and there is no default"
                unless defined $self->{dsproto}->{$ds};
        }
    }

    #----------------------------------------------------------------
    my $dfl = $opt{'default-location'};
    if(defined $dfl) {

        croak "Error: --default-location option: \"$dfl\" is not a valid location"
            unless _validate_location $dfl;

        $self->{default_location} = $dfl;
    }

    my $dsl = $opt{'location'};
    if(defined $dsl) {
        foreach my $i (@$dsl) {
            # This uses the fact that Mu2e dataset names can not contain columns
            my ($ds, $loc) = split /:/, $i, 2;

            croak "Errror: the --location option \"$i\" does not contain a ':'"
                unless defined $loc;

            croak "Error: --location option: \"$loc\" is not a valid location"
                unless _validate_location $loc;

            croak "Error: --location option: \"$ds\" is not on the dataset list.\n"
                ."Known datasets are:\n" . join("\n", @{$self->{dslist}}) . "\n"
                unless 0 + grep { $_ eq $ds } @{$self->{dslist}};

            $loc =~ s|/*$||;

            $self->{dsloc}->{$ds} = $loc;
        }
    }

    # Check that that we have a complete set of location information for the listed datasets
    if(not defined $self->{default_location}) {
        foreach my $ds (@{$self->{dslist}}) {
            croak "Error: location for dataset \"$ds\" is not set and there is no default"
                unless defined $self->{dsloc}->{$ds};
        }
    }

}

sub initialize {
    my ($self, $ds, $opt) = @_;

    croak "Mu2eInSpecs->init(): the second arg must be a reference to cmdline %opts"
        unless ref($opt) eq 'HASH';

    croak "Mu2eInSpecs->init(): the first arg must be a reference to a list of datasets"
        unless ref($ds) eq 'ARRAY';

    $self->_define_datasets(@$ds);
    $self->_parse_useropts(%$opt);
}

1
;
#================================================================
__END__
=head1 NAME

Mu2eInSpecs - information about input protocol and file location for Mu2e datasets.

=head1 DESCRIPTION

The Mu2eInSpecs class provides information about input
protocol and location for files in a pre-defined list
of datasets.  A new instance created with

    my $sp = Mu2eInSpecs->new();

starts with all known protocols (file, root, ifdh) enabled.
One can call

    $sp->disable_protocol($proto);

to turn off protocols that do not make sense in a given application.
After that the use of

    $sp->help_opts($prefix);
    $sp->help_explanation($prefix);

will return blocks of text for use in documenting command line
options of the application, with disabled protocols edited out.
The $prefix above is just a string that is prepended at the beginning
of each line of the text block; usually a number of spaces to line
things up.   Of course there is also a

    $sp->option_defs();

that returns a list of options for use with GetOptions from
Getopt::Long.  Once a list dataset of interest is prepared in @dslist,
and user provided options are retrieved by GetOptions(\%opt, ...), use

   $sp->initialize(\@dslist, \%opt);

to do the processing.  This is where most consistency checks
are done, a successful return from that method means the object
has complete information on protocols and locations for all
the datasets given in the constructor.  (And that the command
line did not mangle dataset names with typos, as dataset
listed on the command line is required to be in @dslist.)

Once an instance has been initialized, it can be queried with

   $sp->protocol($datasetname);
   $sp->location($datasetname);
   $sp->abspathname($file_basename);

=head1 AUTHOR

Andrei Gaponenko, 2023

=cut
