#!/bin/bash
# grep "package" also apply to the files
for thefile in *.java ; do
   grep -v "^import " $thefile > $thefile.$$.tmp
   mv $thefile.$$.tmp $thefile
done
