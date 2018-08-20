#!/bin/sh
#############################################################################################################################
##
##            Script Name : LI_W01A_s3_to_hdfs.sh
##                Purpose : The purpose of this script is to:
##                              1.Copy external dependent files from AWS S3 to local temporary directory.
##                              2.Replace the multiline delimiter with pipe separated delimiter.
##                              3.Copy the files from local temporary directory to hdfs directory.
##                  Usage : ./LI_W01A_s3_to_hdfs.sh sourceS3DirPath destHDFSDirPath
##
#############################################################################################################################

# Main
# Input Parameters
sourceS3DirPath=$1
destHDFSDirPath=$2
destDelimiter=$3

echo "Script Started:$0 $sourceS3DirPath $destHDFSDirPath"
echo "sourceS3DirPath=${sourceS3DirPath}"
echo "destHDFSDirPath=${destHDFSDirPath}"

# Generate local dir for temp processing.
localTmpDir="/tmp/brr-daily-lineitem/LI_W01A/${formattedSegmentDate}"
rm -rf ${localTmpDir} 2>/dev/null
mkdir -p ${localTmpDir} 2>/dev/null
if [ $? -ne 0 ]
then
  echo "Script:$0 failed"
  exit 1
fi
echo "Local Directory: $localTmpDir"

hadoop fs -test -e "${sourceS3DirPath}"
if [ $? -eq 0 ]
then
  # Copy the external files from S3 to local 
  hadoop fs -copyToLocal ${sourceS3DirPath}/* ${localTmpDir}
  if [ $? -ne 0 ]
  then
    echo "hadoop fs -copyToLocal ${sourceS3DirPath}/* ${localTmpDir} failed. Aborting script."
    exit 1
  fi
else
	echo "Files not present at ${sourceS3DirPath}."
	exit 1
fi

# Replace multi character delimiter and pipe delimiter to designated delimiter separated delimited. 
# Pig don't handle multiline character well. 
sed -i -e "s/{^}/${destDelimiter}/g" ${localTmpDir}/*
sed -i -e "s/|/${destDelimiter}/g" ${localTmpDir}/*

# Check for destination hdfs location
hadoop fs -test -d ${destHDFSDirPath}
if [ $? -ne 0 ]
then
  echo "Script:$0 failed"
  echo "${destHDFSDirPath} missing"
  exit 1
fi

hadoop fs -copyFromLocal ${localTmpDir}/*  ${destHDFSDirPath}/

echo "Script:$0 finished"