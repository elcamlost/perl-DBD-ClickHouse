package ClickHouse;
use 5.010;
use strict;
use warnings FATAL => 'all';
our $VERSION = '0.01';

use ClickHouse::Default;

use Net::HTTP;
use URI;
use URI::Escape;
use URI::QueryParam;
use Try::Tiny;

sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;

    $opts{'host'}     ||= 'localhost';
    $opts{'port'}     ||= ClickHouse::Default::PORT();
    $opts{'database'} ||= ClickHouse::Default::DATABASE();
    $opts{'user'}     ||= ClickHouse::Default::USER();

    for my $option_name (qw/password user host port database/) {
        if (defined $opts{$option_name}) {
            $self->{"_$option_name"} = $opts{$option_name};
        }
    }
    $self->{'_builder'} = \&_builder;

    $self->_connect();
    return $self;
}

sub _builder {
    my ($self) = @_;
    delete $self->{'_socket'};
    delete $self->{'_uri'};

    # create Net::HTTP object
    my $socket = Net::HTTP->new(
        'Host'        => $self->{'_host'},
        'PeerPort'    => $self->{'_port'},
        'HTTPVersion' => '1.1',

    ) or die "Can't connect: $@";

    # create URI object
    my $uri = URI->new(sprintf ("http://%s:%d/?database=%s", $self->{'_host'}, $self->{'_port'}, $self->{'_database'}));
    $uri->query_param('user' => $self->{'_user'});
    $uri->query_param('password' => $self->{'_password'}) if $self->{'_password'};

    $self->{'_socket'} = $socket;
    $self->{'_uri'} = $uri;

    return 1;

}

sub _connect {
    my ($self) = @_;
    $self->_builder($self);
    return 1;
}

sub _query {
    my ($self, $cb) = @_;
    return &try (
        $cb,
        catch {
            $self->_connect();
            $cb->();
        }
    );
}


sub disconnect {
    # закрывает соединение
}



sub select {
    my ($self, $query) = @_;

    return $self->_query(sub {
        my $query_url = $self->_construct_query_uri($query);

        $self->{'_socket'}->write_request('GET' => $query_url);
        return $self->_parse_response();
    });

}

sub select_value {
    my ($self, $query) = @_;

    my $arrayref = $self->select($query);
    return $arrayref->[0]->[0];
}

sub do {
    my ($self, $query, @values) = @_;
    return $self->_query(sub {
        my $query_url = $self->_construct_query_uri($query);
        my $post_data = scalar @values ? join (",", map { "(" . join (",", @{ $_ }) . ")" } @values) : "\n" ;

        $self->{'_socket'}->write_request('POST' => $query_url, $post_data);
        return $self->_parse_response();
    });

}

sub ping {
    my ($self) = @_;
    eval {
        $self->{_socket}->write_request('GET' => '/');
    };

    my ($code) = eval { $self->{'_socket'}->read_response_headers() };
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
    my ($code, $mess) = $self->{'_socket'}->read_response_headers();
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
        my $n = $self->{'_socket'}->read_entity_body($buf, 1024);
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

    my $query_uri = $self->{'_uri'}->clone();
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

