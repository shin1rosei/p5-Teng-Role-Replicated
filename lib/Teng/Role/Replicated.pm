package Teng::Role::Replicated;

use strict;
use warnings;
use utf8;

our $VERSION = '0.01';

use Any::Moose '::Role';

use Clone qw(clone);
use Carp ();

use Scalar::Util qw/blessed/;

use Teng::Role::Replicated::ScopeGuard;

has slave => (
    is       => 'rw',
);

has force_master => (
    is      =>  'rw',
    default => 0,
);

has skip_slave => (
    is       => 'ro',
);

has force_master_scopes => (
    is      => 'rw',
    lazy    => 1,
    default => sub {[]},
);

around new => sub {
    my $orig  = shift;
    my $class = shift;

    my %args  = @_ == 1 ? %{$_[0]} : @_;

    my $dbh       = $args{dbh}       || DBI->connect(@{$args{connect_info}})
        or Carp::croak("'dbh' or 'connect_info' is required.");

    my $slave_dbh = $args{slave_dbh} || DBI->connect(@{$args{slave_connect_info}})
        or Carp::croak("'dbh' or 'connect_info' is required.");

    $args{dbh}    = $dbh; delete($args{connect_info});
    my $self      = $class->$orig(%args);

    my $slave_setting              = clone(\%args);
    $slave_setting->{dbh}          = $slave_dbh;
    $slave_setting->{skip_slave}   = 1;
    $self->slave($class->$orig($slave_setting));

    for my $method (qw/single search_named search_by_sql/, @{$args{slave_methods}}) {
        $self->meta->add_around_method_modifier(
            $method => sub {
                my $orig = shift;
                my $me   = shift;

                if ($me->force_master ||
                    $me->txn_manager->in_transaction || 
                    $me->skip_slave) {

                    return $me->$orig(@_);
                }


                if (wantarray) {
                    my @res = $me->slave->$method(@_);
                    my $r   = $self->_traverse_teng(\@res, $me);

                    return @$r;
                }

                my $res = $me->slave->$method(@_);

                return $self->_traverse_teng($res, $me);
            },
        );
    }

    $self;
};

sub force_master_scope {
    return  Teng::Role::Replicated::ScopeGuard->new(@_);
}


sub _start_force_master {
    my ($self, %args) = @_;

    my $caller = $args{caller} || [ caller(0) ];
    my $scopes = $self->force_master_scopes;

    my $rc     = 1;
    if (@$scopes == 0) {
        $rc = $self->force_master(1);
    }
    if ($rc) {
        push @$scopes, { caller => $caller, pid => $$ };
    }
}

sub _end_force_master {
    my $self   = shift;
    my $scopes = $self->force_master_scopes;
    return unless @$scopes;

    my $current = pop @$scopes;
    if (@$scopes == 0) {
        $self->force_master(0);
    }
}

sub _traverse_teng {
    my ($self, $list, $me) = @_;

    if (ref $list eq 'ARRAY') {
        for my $s (@$list) {
            if (blessed $s && $s->{teng}) {
                $s->{teng} = $me;
            }
            if (ref $s eq 'ARRAY') {
                $self->_traverse_teng($s, $me);
            }
        }
    }
    elsif (blessed $list && $list->{teng}) {
        $list->{teng} = $me;
    }

    return $list;
}

1;
__END__

=head1 NAME

Teng::Role::Replicated - Replicated database support for Teng

=head1 SYNOPSIS

  package MyTeng;
  use parent 'Teng';

  use Any::Moose;
  with 'Teng::Role::Replicated';


  my $teng = MyTeng->new([
    connect_info       => [...], # master db connect info
    slave_connect_info => [...], # slave db connect info
  ])

  $teng->single(...);        # for slave db
  $teng->insert(...);        # for master db

  {
    my $scope = $teng->force_master_scope;
    $teng->search(...)->all; # for master db
    $scope->end;
    $teng->search(...)->all; # for slave db
  }

  {
    my $txn = $teng->txn_scope;
    $teng->search(...)->all; # for master db
    $teng->insert(...);      # for master db
    $txn->commit;
    $teng->search(...)->all; # for slave db
  }

=head1 DESCRIPTION

=head1 METHODS

=head2 force_master_scope

return Teng::Role::Replicated::ScopeGuard instance. 

As long as this instance is alive or is not called "end" method, it does not executed SQL to slave. 

=head1 AUTHOR

Shinichiro Sei E<lt>shin1rosei@kayac.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

copyright (c) 2012 kayac inc. all rights reserved.

this program is free software; you can redistribute
it and/or modify it under the same terms as perl itself.

the full text of the license can be found in the
license file included with this module.

=cut
