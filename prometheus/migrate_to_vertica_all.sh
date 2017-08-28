export PATH=$PATH:/opt/vertica/bin

# This script takes 1 command line argument:  the schema name.  
# It assumes that the source and target schema names are identical.


for i in `mysql -uepbuilder -p'Test1231!' -hhciprddb08 -Nnqsre "use $1;show tables;"`
do
echo $i
mysql -uepbuilder -p'Test1231!' -hhciprddb08 -Nnqsre "use $1; select * from $i;" |  sed 's/,/ /g'  |  sed 's/\t/|/g' | sed 's/NULL//g' | vsql -Uepbuilder -w'Test1231!' -hhciprddb01 -p5433 -dhpci -c "COPY $1.$i FROM local STDIN DELIMITER '|' EXCEPTIONS 'exception_$i.log' REJECTED DATA 'rejected_$i.log' DIRECT ;"
done

