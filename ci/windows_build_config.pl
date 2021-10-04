use strict;
use warnings;

our $config;

$config->{"tap_tests"} = 1;
$config->{"asserts"} = 1;
$config->{"openssl"} = "c:/openssl/1.1.1l/";
$config->{"perl"} = "c:/perl/strawberry-$ENV{PERL_VERSION}-64bit/perl";

1;
