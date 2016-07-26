package ClickHouse;
use 5.010;
use strict;
use warnings FATAL => 'all';
our $VERSION = '0.01';

use Net::HTTP;
use URI;
use URI::Escape;
use URI::QueryParam;
use Carp;
use Scalar::Util qw/looks_like_number/;

our $AUTOLOAD;

sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;
    $self->_init(%opts);
    return $self;
}

{
    my %_attrs = (
        '_host'       => 'localhost',
        '_port'       => 8123,
        '_database'   => 'default',
        '_user'       => ':',
        '_password'   => 48,
        '_keep_alive' => 1,
        '_format'     => 'TabSeparated',
        '_socket'     => undef,
        '_uri'        => undef,
    );

    #
    # CLASS METHODS
    #
    # Returns a copy of the instance.
    sub _clone {
        my ($self)  = @_;
        my ($clone) = {%$self};
        bless( $clone, ref $self );
        return ($clone);
    }

    # Verify that an attribute is valid (called by the AUTOLOAD sub)
    sub _accessible {
        my ( $self, $name ) = @_;
        if ( exists $_attrs{$name} ) {

            #$self->verbose("attribute $name is valid");
            return 1;
        }
        else { return 0; }
    }

    # Initialize the object (only called by the constructor)
    sub _init {
        my ( $self, %args ) = @_;

        foreach my $key ( keys %_attrs ) {
            $key =~ s/^_+//;
            if ( defined ($args{$key}) && $self->_accessible( "_$key" ) ) {
                $self->{"_$key"} = $args{$key};
            }
            else {
                $self->{"_$key"} = $_attrs{"_$key"};
            }
        }
        # create Net::HTTP object
        my $socket = Net::HTTP->new(
            'Host'        => $self->{'_host'},
            'PeerPort'    => $self->{'_port'},
            'HTTPVersion' => '1.1',
            'KeepAlive'   => $self->{'_keep_alive'},
        ) or die "Can't connect: $@";

        # create URI object
        my $uri = URI->new(sprintf ("http://%s:%d/?database=%s", $self->{'_host'}, $self->{'_port'}, $self->{'_database'}));
        $uri->query_param('user' => $self->{'_user'});
        $uri->query_param('password' => $self->{'_password'}) if $self->{'_password'};

        $self->{'_socket'} = $socket;
        $self->{'_uri'} = $uri;

        return 1;
    }
}

sub ClickHouse::AUTOLOAD {
    no strict 'refs';
    my ( $self, $value ) = @_;
    if ( ( $AUTOLOAD =~ /.*::_get(_\w+)/ ) && ( $self->_accessible($1) ) ) {
        my $attr_name = $1;
        *{$AUTOLOAD} = sub { return $_[0]->{$attr_name} };
        return ( $self->{$attr_name} );
    }
    if ( $AUTOLOAD =~ /.*::_set(_\w+)/ && $self->_accessible($1) ) {
        my $attr_name = $1;
        *{$AUTOLOAD} = sub { $_[0]->{$attr_name} = $_[1]; return; };
        $self->{$1} = $value;
        return;
    }
    croak "No such method: $AUTOLOAD";
}

sub DESTROY {}

sub disconnect {
    my ($self) = @_;
    my $socket = $self->_get_socket();
    $socket->keep_alive(0);
    $self->ping();

    return 1;
}



sub select {
    my ($self, $query) = @_;

    my $query_url = $self->_construct_query_uri($query);

    $self->_get_socket()->write_request('GET' => $query_url);
    return $self->_parse_response();

}

sub select_value {
    my ($self, $query) = @_;

    my $arrayref = $self->select($query);
    return $arrayref->[0]->[0];
}

sub do {
    my ($self, $query, @values) = @_;
    my $query_url = $self->_construct_query_uri($query);
    my $post_data = scalar @values ? join (",", map { "(" . join (",", @{ $_ }) . ")" } @values) : "\n" ;

    $self->_get_socket()->write_request('POST' => $query_url, $post_data);
    return $self->_parse_response();

}

sub ping {
    my ($self) = @_;

    my ($code) = eval {
        $self->_get_socket()->write_request('GET' => '/');
        $self->_get_socket()->read_response_headers();
    };

    if ($@) {
        return 0;
    }
    unless ($code == 200) {
        return 0;
    }
    my $result = $self->_read_body();
    unless ($result->[0] eq 'Ok.' ) {
        return 0;
    }
    return 1;
}

sub _parse_response {
    my ($self) = @_;
    my ($code, $mess) = $self->_get_socket()->read_response_headers();
    if ($code == 200 ) {
        return _formaty_query_result( $self->_read_body() );
    } else {
        my $add_mess = _formaty_query_result( $self->_read_body() );
        if (defined $add_mess) { $add_mess = $add_mess->[0]->[0] };
        die "ClickHouse error: $mess ($add_mess)";
    }
}

sub _read_body {
    my ($self) = @_;

    my @response;
    while (1) {
        my $buf;
        my $n = $self->_get_socket()->read_entity_body($buf, 1024);
        die "can't read response: $!" unless defined $n;
        last unless $n;
        push @response, split (/\n/, $buf);
    }
    return \@response;
}

sub _formaty_query_result {
    my ($query_result) = @_;
    return [ map { [ split (/\t/) ] } @{ $query_result } ];
}


sub _construct_query_uri {
    my ($self, $query) = @_;

    my $query_uri = $self->_get_uri()->clone();
    $query_uri->query_param('query' => $query);

    return $query_uri->as_string();
}


1;

__END__

=pod

=encoding utf8

=head1 NAME

DBD::ClickHouse - Database driver for Clickhouse OLAP Database

=head1 EXAMPLE

    use ClickHouse;

    my $ch = ClickHouse->new(
        host     => $ENV{'CLICK_HOUSE_HOST'}
        port     => 8123,
        user     => 'Frodo'
        password => 'finger',

    );

    my $rows = $ch->select("SELECT id, field_one, field_two FROM some_table");

    for my $row (@$rows) {
        # Do something with your row
    }

    $ch->do("INSERT INTO some_table (id, field_one, field_two) VALUES",
        [1, "String value", 38962986],
        [2, "String value", 38962986],
    );

