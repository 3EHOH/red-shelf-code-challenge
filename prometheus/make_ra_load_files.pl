#!/usr/bin/perl

# ****************************************************************************************
sub Load_RA_Exp_Cost {
	open(OUT,">ra_exp_cost.sql");

	open (FH,"<ra_exp_cost.csv");
	$firstline = <FH>;

	chomp($firstline);

	$firstline =~ s/\"//g;

	@fields = split(',', $firstline);

	print OUT "create table ra_exp_cost (\n";

	$c = 0;
	$tc = scalar(@fields);

	foreach $fname (@fields){
		if($c == 0){
			$fname = "row_names";
		}

		if($c<4){
			$ftype = "varchar(255) default null";
		}else{
			$ftype = "float default null";
		}

		$c++;

		if($c < $tc){
			$comma = ",";
		}else{
			$comma = "";
		}

		print OUT "$fname $ftype $comma\n";

	}

	print OUT ");\n";

	close FH;
        close OUT;
}
# ****************************************************************************************
sub Load_RA_Coeffs {
	open (OUT,">ra_coeffs.sql");

        open (FH,"<ra_coeffs.csv");
        $firstline = <FH>;

        chomp($firstline);

        $firstline =~ s/\"//g;

        @fields = split(',', $firstline);

        print OUT "create table ra_coeffs (\n";

        $c = 0;
        $tc = scalar(@fields);

        foreach $fname (@fields){
                if($c == 0){
                        $fname = "row_names";
                }

                $ftype = "varchar(255) default null";

                $c++;

                if($c < $tc){
                        $comma = ",";
                }else{
                        $comma = "";
                }

                print OUT "$fname $ftype $comma\n";

        }

        print OUT ");\n";

        close FH;
	close OUT;
}

# ****************************************************************************************
sub Load_RA_Final_Data {
	open (OUT,">ra_final_data.sql");

        open (FH,"<ra_final_data.csv");
        $firstline = <FH>;

        chomp($firstline);

        $firstline =~ s/\"//g;

        @fields = split(',', $firstline);

        print OUT "create table ra_final_data (\n";

        $c = 0;
        $tc = scalar(@fields);

        foreach $fname (@fields){
                if($c == 0){
                        $fname = "row_names";
                }

                if($c<5){
                        $ftype = "varchar(255) default null";
		}elsif ($c > 4 and $c < 9){
			$ftype = "integer default null";
                }else{
                        $ftype = "float default null";
                }

                $c++;

                if($c < $tc){
                        $comma = ",";
                }else{
                        $comma = "";
                }

                print OUT "$fname $ftype $comma\n";

        }

        print OUT ");\n";

        close FH;
	close OUT;
}
# ****************************************************************************************
sub Print_Load_Statements {

	print "cat ra_exp_cost.csv | sed 's/\"//g'  | sed 's/NULL//g' | sed 's/NA/0/g' | vsql -U$dbuser -w\'$dbpass\' -h$dbhost -p5433 -d$db -c \"COPY $dbschema.ra_exp_cost FROM local STDIN DELIMITER \',\' EXCEPTIONS \'exception_ra_exp_cost.log\' REJECTED DATA \'rejected_ra_exp_cost.log\' DIRECT SKIP 1;\"\n";

	print "\n";

	print "cat ra_coeffs.csv | sed 's/\"//g' | sed 's/NULL//g' | sed 's/NA/0/g' | vsql -U$dbuser -w\'$dbpass\' -h$dbhost -p5433 -d$db -c \"COPY $dbschema.ra_coeffs FROM local STDIN DELIMITER \',\' EXCEPTIONS \'exception_ra_coeffs.log\' REJECTED DATA \'rejected_ra_coeffs.log\' DIRECT SKIP 1;\"\n";

	print "\n";

	print "cat ra_final_data.csv |sed 's/\"//g'  | sed 's/NULL//g' | sed 's/NA/0/g' | vsql -U$dbuser -w\'$dbpass\' -h$dbhost -p5433 -d$db -c \"COPY $dbschema.ra_final_data FROM local STDIN DELIMITER \',\' EXCEPTIONS \'exception_ra_final_data.log\' REJECTED DATA \'rejected_ra_final_data.log\' DIRECT SKIP 1;\"\n";

}
# ****************************************************************************************

$db = "hpci";
$dbschema = "epbuilder";
$dbhost = "hciprddb01";
$dbuser = "epbuilder";
$dbpass = "Test1231!";



print "\n";
& Load_RA_Exp_Cost;
print "\n";
& Load_RA_Coeffs;
print "\n";
& Load_RA_Final_Data;
print "\n";
print "\n";
& Print_Load_Statements;
print "\n";
