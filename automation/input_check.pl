#!/usr/bin/perl

use strict;

use Scalar::Util qw(looks_like_number);


my %columnCounts = ();
my %columnValues = ();
my %columnTypes = ();

my @columnList = ();

open my $input_file, "<", $ARGV[0];

my $lineCount = 0;

foreach my $line (<$input_file>) {
    chomp $line;
    if ($lineCount == 0) {
        @columnList = split ",", $line;
        for my $column (@columnList) {
            $columnCounts{$column} = 0;
            $columnValues{$column} = {};
        }
    }
    else {
        my @lineColumns = split ",", $line;
        for (my $i = 0; $i < scalar(@lineColumns); $i++) {
            my $value = $lineColumns[$i];
            my $column = $columnList[$i];
            if ($value ne '') {
                $columnCounts{$column}++;
                
                # figure out type
                if (looks_like_number($value)) {
                    if ($value =~ /\./) {
                        # check for decimal
                        $columnTypes{$column} = 'Decimal';
                    }
                    else {
                        $columnTypes{$column} = 'Integer';
                    }
                }
                elsif ($value =~ /\d\d\d\d-\d\d-\d\d/) {
                    # check for dates
                    $columnTypes{$column} = 'Date';
                }
                else {
                    $columnTypes{$column} = 'String';
                }
                
                # increase count of unique values
                if (not exists $columnValues{$column}) {
                    $columnValues{$column} = {};
                }
                if (not exists $columnValues{$column}{$value}) {
                    $columnValues{$column}{$value} = 0;
                }
                $columnValues{$column}{$value} += 1;
            }
        }
    
        if ($lineCount % 10000 == 0) {
            print "processing record $lineCount: $line\n";
        }
    }
        
    $lineCount++;
}

# print output
print "Column,count,type,percentage,uniques,samples,sampleCount\n";
print "Total," . ($lineCount - 1) . ",\n";

foreach my $column (@columnList) {
    my $count = $columnCounts{$column};
    my $type = $columnTypes{$column};
    print "$column,$count,$type,";
    if (exists $columnValues{$column} and scalar(keys($columnValues{$column})) > 0) {
        printf "%.2f\%,\%d\n", ($count / $lineCount) * 100, scalar(keys %{ $columnValues{$column} });
        if ($type ne 'Integer' and $type ne 'Decimal') {
            my $i = 0;
            foreach my $key (sort { $a > $b } keys(%{ $columnValues{$column} })) {
                if ($i < 5) {
                    print ",,,,,$key,$columnValues{$column}{$key}\n";
                }
                $i++;
            }
        }
    }
    else {
        print "\n";
    }
}

