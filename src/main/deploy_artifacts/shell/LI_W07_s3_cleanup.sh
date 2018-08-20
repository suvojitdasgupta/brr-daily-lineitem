#!/bin/sh
#############################################################################################################################
##
##            Script Name : LI_W07_s3_cleanup.sh
##                Purpose : The purpose of this script is to cleanup the target S3 directories for copy.
##                  Usage : ./LI_W07_s3_cleanup.sh <hdfsDirs>
##
##
#############################################################################################################################

for s3Dir in ${@:1}
do
   hadoop fs -test -d ${s3Dir}
   if [ $? -eq 0 ]
   then
       hadoop fs -rm -r ${s3Dir}
       if [ $? -ne 0 ]
       then
           echo "Script:$0 failed"
           exit 1
       fi
       echo  "${s3Dir} deleted"
   else
       echo "${s3Dir} is not present. Nothing to delete"
   fi
done

echo "Script:$0 executed successfully"