#!/bin/sh
#############################################################################################################################
##
##            Script Name : LI_W07_update_status.sh
##                Purpose : The purpose of this script is to cleanup the target S3 directories for copy.
##                  Usage : ./LI_W07_update_status.sh $segmentDate $s3Dir
##
#############################################################################################################################

segmentDate=$1
s3StatusDir=$2
curr_utc_date=`date -u +%Y-%m-%d_%H-%M-%S-%N`

hadoop fs -mkdir -p "${s3StatusDir}" 2>/dev/null
hadoop fs -touchz "${s3StatusDir}/DONE"

if [ $? -eq 0 ]
then
  echo "Script:$0 executed successfully"
  exit 0
else
  echo "Script:$0 failed"
  exit 1
fi
