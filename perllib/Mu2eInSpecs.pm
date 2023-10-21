# run perldoc on this file or see POD at the end

use strict;
use warnings;

package Mu2eInSpecs;
use Exporter qw ( import );
use Carp;
use Data::Dumper; # for debugging

use constant proto_file => 'file';
use constant proto_ifdh => 'ifdh';
use constant proto_root => 'root';

my @all_protocols = ( proto_file, proto_ifdh, proto_root );

#================================================================
sub new {
    my $class = shift;
    my ( $dsref ) = @_;

    croak "Mu2eInSpecs constructor should not be called on an existing instance"
        if ref $class;

    # Freeze the list of datasets in the constructor so that consistency
    # checks can be done when cmdline options are parsed.
    croak "Error: Mu2eInSpecs constructor's first argument"
        ." must be a reference to an array of dataset names"
        unless defined $dsref;

    my $self = bless {
        dslist => $dsref,
        protocols => [ @all_protocols ],
        dsproto => {}, # individual ds to protocol settings
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
        );
}

sub help_opts {
    my ($self, $prefix) = @_;
    $prefix = '' unless defined $prefix;

    return _prefix_text_block $prefix, <<EOF
[--default-protocol <protocol>]\\
[--protocol <dataset>:<protocol>]\\
EOF
        ;
}

sub help_explanation {
    my ($self, $prefix) = @_;
    $prefix = '' unless defined $prefix;

    return _prefix_text_block $prefix, <<EOF
--default-protocol  <protocol>
  Sets the default file access protocol that can be overriden for individual
  datasets.  The possible values of <protocol> are

EOF
. _prefix_text_block(' 'x8, $self->_help_on_protocols())
. <<EOF
--protocol  <dsname>:<protocol>
  Use the specified protocol to access files from the given dataset.
EOF
        ;
}

#================================================================
sub parse_useropts {
    my $self = shift;
    my %opt = @_;

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

    # Check that that we have a complete set of information for the listed datasets
    if(not defined $self->{default_proto}) {
        foreach my $ds (@{$self->{dslist}}) {
            croak "Error: protocol for dataset \"$ds\" is not set and there is no default"
                unless defined $self->{dsproto}->{$ds};
        }
    }
}

1
;
#================================================================
__END__
=head1 NAME

Mu2eInSpecs - information about input protocol and file location for Mu2e datasets.

=head1 DESCRIPTION

The Mu2eInSpecs class takes a list of dataset names in its
constructor, processes options given on the command line that define
protocol and location of files from each of the datasets, and then can
be queried to tell location and input protocol for each of the
datasets.   A new instance created with

    my $sp = Mu2eInSpecs->new(\@dslist);

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
Getopt::Long.   Once the user provided options are retrieved
by GetOptions(\%opt, ...),  use

   $sp->parse_useropts(%opt);

to do the processing.  This is where most consistency checks
are done, a successful return from that method means the object
has complete information on protocols and locations for all
the datasets given in the constructor.  (And that the command
line did not mangle dataset names with typos, any dataset
listed on the command line must be on the original @dslist.)

=head1 AUTHOR

Andrei Gaponenko, 2023

=cut
