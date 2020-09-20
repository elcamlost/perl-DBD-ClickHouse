#!perl

use 5.010;
use strict;
use warnings;

use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan skip_all => "Author tests not required for installation";
}

BEGIN {
    use_ok 'ClickHouse' or die;
}

diag "Testing ClickHouse $ClickHouse::VERSION, Perl $], $^X";

my $db = new_ok 'ClickHouse';
$db->ping or die 'local instance not running, unable to run tests';

my $query = 'show databases';
is_deeply $db->selectall_arrayref($query),
    $db->select($query),
    'selectall_arrayref() OK';

is_deeply $db->selectall_arrayref($query, { Columns => {} }),
    [ map { {name => $_->[0]} } @{ $db->select($query) } ],
    'selectall_arrayref(query, {Columns => {}}) OK';

is_deeply $db->selectcol_arrayref($query),
    [ map { $_->[0] } @{ $db->select($query) } ],
    'selectcol_arrayref() OK';

done_testing;

__END__
