package Teng::Role::Replicated::ScopeGuard;

use strict;
use warnings;
use utf8;

sub new {
    my ($class, $obj, %args) = @_;
    my $caller = $args{caller} || [ caller(1) ];
    $obj->_start_force_master(caller => $caller);
    bless [ 0, $obj, $caller, ], $class;
}

sub end {
    return if $_[0]->[0];
    $_[0]->[1]->_end_force_master;
    $_[0]->[0] = 1;
}

sub DESTROY {
    my ($dismiss, $obj, $caller) = @{$_[0]};

    return if $dismiss;

    $obj->_end_force_master;
}

1;
