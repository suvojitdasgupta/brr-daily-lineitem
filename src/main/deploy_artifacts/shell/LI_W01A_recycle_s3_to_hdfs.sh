#!/bin/sh
#############################################################################################################################
##
##            Script Name : LI_W01A_recycle_s3_to_hdfs.sh
##                Purpose : The purpose of this script is to copy recycle file from AWS S3
##                          to corresponding hdfs directory.
##                  Usage : ./LI_W01A_recycle_s3_to_hdfs.sh sourceS3DirPath destHDFSDirPath
##
##
#############################################################################################################################

# Function
function execute_command(){
	local cmd=$1
	echo -e "\nExecuting, ${cmd} \n"
	eval "${cmd}"
}

# Main
# Input Parameters
sourceS3DirPath=$1
destHDFSDirPath=$2

echo "Script Started:$0 $sourceS3DirPath $destHDFSDirPath"
echo "sourceS3DirPath=${sourceS3DirPath}"
echo "destHDFSDirPath=${destHDFSDirPath}"

# Cleanup destination location
execute_command "hadoop fs -rm -r ${destHDFSDirPath}/LI_18_process_brr_rejects_recycle 2>/dev/null"
execute_command "hadoop fs -mkdir -p ${destHDFSDirPath}/LI_18_process_brr_rejects_recycle"

hadoop fs -test -e "${sourceS3DirPath}/LI_18_process_brr_rejects_recycle/"
if [ $? -eq 0 ];
then
    echo "Recycle file for previous date exists. Copying the recycle file"
	execute_command "hadoop fs -cp ${sourceS3DirPath}/LI_18_process_brr_rejects_recycle/* ${destHDFSDirPath}/LI_18_process_brr_rejects_recycle/"
	execute_command "hadoop fs -touchz ${destHDFSDirPath}/LI_18_process_brr_rejects_recycle/"
else
	echo "Recycle file for previous date doesn't exist. Creating an empty recycle file"
	execute_command "hadoop fs -touchz ${destHDFSDirPath}/LI_18_process_brr_rejects_recycle/part-m-00000"
fi

echo "Script:$0 finished"