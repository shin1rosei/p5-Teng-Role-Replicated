use inc::Module::Install;
name 'Teng-Role-Replicated';
all_from 'lib/Teng/Role/Replicated.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
