#!/usr/bin/perl
# Copyright (c) 2026, PostgreSQL Global Development Group

# Read NLS metadata for Make.  Emit assignments separated by |, since
# $(shell) replaces newlines with spaces.  No generated file is needed.
use strict;
use warnings;
use JSON::PP;

my ($common_file, $catalog_file) = @ARGV;
die "usage: $0 COMMON [CATALOG]\n" unless defined $common_file;

sub read_json
{
	my ($path) = @_;
	open(my $in, '<', $path) or die "$path: $!\n";
	local $/;
	return decode_json(<$in>);
}

my $common = read_json($common_file);

sub expand
{
	my ($values, @parents) = @_;
	die "expected a list of NLS values\n" unless ref($values) eq 'ARRAY';
	my @result;
	foreach my $value (@$values)
	{
		die "invalid NLS value\n"
		  if ref($value) || !defined($value) || $value !~ m{\A[\w./:@,+-]+\z};
		if ($value =~ /^\@([a-z_]+)\@$/)
		{
			my $key = $1;
			die "unknown common list \"$key\"\n"
			  unless exists $common->{$key};
			die "recursive common list \"$key\"\n"
			  if grep { $_ eq $key } @parents;
			push @result, expand($common->{$key}, @parents, $key);
		}
		else
		{
			$value =~ s/\@source\@/\$(top_srcdir)/g;
			push @result, $value;
		}
	}
	return @result;
}

my %values;
if (defined $catalog_file)
{
	my $catalog = read_json($catalog_file);
	die "$catalog_file: invalid catalog name\n"
	  unless defined($catalog->{name})
	  && $catalog->{name} =~ /^[a-zA-Z0-9_-]+$/;
	$values{CATALOG_NAME} = $catalog->{name};
	$values{GETTEXT_FILES} =
	  exists $catalog->{scan}
	  ? '+ gettext-files'
	  : join(' ', expand($catalog->{files}));
	$values{GETTEXT_SCAN} = join(' ', expand($catalog->{scan} // []));
	$values{GETTEXT_TRIGGERS} = join(' ', expand($catalog->{keywords} // []));
	$values{GETTEXT_FLAGS} = join(' ', expand($catalog->{flags} // []));
}
else
{
	# Compatibility with external PGXS nls.mk files.
	foreach my $kind ('frontend', 'backend')
	{
		foreach my $field ('files', 'keywords', 'flags')
		{
			my $key = "${kind}_${field}";
			next unless exists $common->{$key};
			my $suffix = $field eq 'keywords' ? 'TRIGGERS' : uc($field);
			$values{ uc($kind) . "_COMMON_GETTEXT_$suffix" } =
			  join(' ', expand($common->{$key}));
		}
	}
}

# Print only after all metadata has been read and expanded successfully.
print join('|', map { "$_ := $values{$_}" } sort keys %values), "\n"
  or die "could not write NLS settings: $!\n";
