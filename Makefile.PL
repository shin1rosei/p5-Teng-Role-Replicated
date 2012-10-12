use inc::Module::Install;
name 'Teng-Role-Replicated';
all_from 'lib/Teng/Role/Replicated.pm';

requires 'Any::Moose' => 0;
requires 'Clone'      => 0;

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
