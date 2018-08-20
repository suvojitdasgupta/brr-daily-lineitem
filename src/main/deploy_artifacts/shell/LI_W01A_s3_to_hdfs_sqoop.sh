#!/bin/sh
#############################################################################################################################
##
##            Script Name : LI_W01A_s3_to_hdfs_sqoop.sh
##                Purpose : The purpose of this script is to:
##                              1.Copy sqoop dependent files from AWS S3 to HDFS
##                          This script is only invoked as part of recovery runs.
##                  Usage : ./LI_W01A_s3_to_hdfs_sqoop.sh sourceS3DirPath destHDFSDirPath
##
#############################################################################################################################

# Main
# Input Parameters
sourceS3DirPath=$1
destHDFSDirPath=$2

echo "Script Started:$0 $sourceS3DirPath $destHDFSDirPath"
echo "sourceS3DirPath=${sourceS3DirPath}"
echo "destHDFSDirPath=${destHDFSDirPath}"

hadoop fs -test -d ${destHDFSDirPath}
if [ $? -ne 0 ]
then
  echo "${destHDFSDirPath} is missing. Creating the ${destHDFSDirPath} now."
  hadoop fs -mkdir -p ${destHDFSDirPath} 2>/dev/null
fi

hadoop fs -test -e "${sourceS3DirPath}"
if [ $? -eq 0 ]
then
  # Copy the external files from S3 to hdfs 
  hadoop fs -cp ${sourceS3DirPath}/* ${destHDFSDirPath}/
  if [ $? -ne 0 ]
  then
    echo "Script:$0 failed"
    echo "hadoop fs -cp ${sourceS3DirPath}/* ${destHDFSDirPath}/ failed. Aborting script."
    exit 1
  fi
else
	echo "Files not present at ${sourceS3DirPath}."
	exit 1
fi

echo "Script:$0 finished"