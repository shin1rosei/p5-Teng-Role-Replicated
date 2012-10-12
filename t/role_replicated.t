use strict;
use Test::More;

{
    package t::Teng;
    use parent 'Teng';

    use Any::Moose;
    with 'Teng::Role::Replicated';

    package t::Teng::Schema;
    use Teng::Schema::Declare;

    table {
        name 'foo';
        pk 'id';
        columns qw(id);
    };
}

my $teng = new_ok 't::Teng', [ 
    connect_info       => [ 'dbi:SQLite:', '', '' ], 
    slave_connect_info => [ 'dbi:SQLite:', '', '' ]
];

$teng->do('CREATE TABLE foo ( id INT AUTO_INCREMENT PRIMARY KEY )');
$teng->slave->do('CREATE TABLE foo ( id INT AUTO_INCREMENT PRIMARY KEY )');

$teng->fast_insert(foo => { id => 1 });
$teng->fast_insert(foo => { id => 2 });
$teng->fast_insert(foo => { id => 3 });

is scalar @{[ $teng->search('foo')->all ]}, 0;
is scalar @{[ $teng->slave->search('foo')->all ]}, 0;

{
    my $txn = $teng->txn_scope;
    is scalar @{[ $teng->search('foo')->all ]}, 3;
    $txn->commit;
}
$teng->slave->fast_insert(foo => {id => 3});

my $foo = $teng->single('foo', {id => 3});
ok $foo;

$foo->update({ id => 4 });

is scalar @{[ $teng->slave->search('foo', {id => 4})->all ]}, 0;

is ref $teng->search('foo', {id => 3}), 'Teng::Iterator';

my @t = $teng->search('foo', {id => 3});
is @t, 1;
is ref $t[0], 't::Teng::Row::Foo';

{
    my $scope = $teng->force_master_scope;
    is scalar @{[ $teng->search('foo')->all ]}, 3;
    $scope->end;
    is scalar @{[ $teng->search('foo')->all ]}, 1;
}

{
    my $scope = $teng->force_master_scope;
    is scalar @{[ $teng->search('foo')->all ]}, 3;
}
is scalar @{[ $teng->search('foo')->all ]}, 1;


$foo = $teng->insert(foo => { id => 10 });
ok $foo, 'insert and refetch';

done_testing;
