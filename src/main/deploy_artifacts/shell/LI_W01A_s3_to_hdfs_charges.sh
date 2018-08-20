#!/bin/sh
#############################################################################################################################
##
##            Script Name : LI_W01A_s3_to_hdfs_charges.sh
##                Purpose : The purpose of this script is to:
##                              1.Copy external Charges dependent files from AWS S3 to HDFS.
##                  Usage : ./LI_W01A_s3_to_hdfs_charges.sh destHDFSDirPath <sourceS3DirPathFiles>
##
#############################################################################################################################

# Main
# Input Parameters
destHDFSDirPath=$1

echo "Script Started:$0 $destHDFSDirPath ${@:2}"
echo "destHDFSDirPath=${destHDFSDirPath}"
echo "sourceS3DirPathFiles=${@:2}"

hadoop fs -test -d ${destHDFSDirPath}
if [ $? -ne 0 ]
then
  echo "${destHDFSDirPath} is missing. Creating the ${destHDFSDirPath} now."
  hadoop fs -mkdir -p ${destHDFSDirPath} 2>/dev/null
fi

for sourceS3DirPathFile in ${@:2}
do
  hadoop fs -test -d ${sourceS3DirPathFile}
  if [ $? -ne 0 ]
  then
    echo "Script:$0 failed"
    echo "${sourceS3DirPathFile} missing. Aborting script."
    exit 1
  fi
    
  echo "hadoop fs -cp ${sourceS3DirPathFile} ${destHDFSDirPath}"
  hadoop fs -cp ${sourceS3DirPathFile} ${destHDFSDirPath}

done

echo "Script:$0 finished"