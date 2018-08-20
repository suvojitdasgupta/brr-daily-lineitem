#!/bin/sh
#############################################################################################################################
##
##            Script Name : LI_W07_merge_hdfs.sh
##                Purpose : The purpose of this script is to merge all part files of a hdfs file and create a single file.
##                  Usage : ./LI_W07_merge_hdfs.sh $formattedSegmentDate $brrLineItemFinalDir <hdfsDirFileNames>
##
#############################################################################################################################

# Input Parameters
formattedSegmentDate=$1
brrLineItemFinalDir=$2
destDelimiter=$3

echo "Script:$0 started"

# Generate local dir for tmp processing.
localTmpDir="/tmp/brr-daily-lineitem/LI_W07/${formattedSegmentDate}"

# Cleanup and Create the local target location
rm -rf ${localTmpDir} 2>/dev/null
mkdir -p ${localTmpDir}/final 2>/dev/null
if [ $? -ne 0 ]
then
  echo "Script:$0 failed"
  exit 1
fi
echo "Local Directory is: $localTmpDir"

for hdfsInputDir in ${@:4}
do
  hadoop fs -copyToLocal -ignoreCrc ${hdfsInputDir}/ ${localTmpDir}/
  if [ $? -eq 0 ]
  then
      echo "hdfs files copied to local successfully."
  else
      echo "hdfs files copy to local failed."
  fi

  tailDir=$(basename ${hdfsInputDir})
  mkdir -p ${localTmpDir}/final/${tailDir}/
  
  localFileNameList=$(ls ${localTmpDir}/${tailDir}/)
  for localFileName in ${localFileNameList}
  do
     localFilePartPathNameList=$(ls ${localTmpDir}/${tailDir}/${localFileName}/part*)

     for localFilePartPathName in ${localFilePartPathNameList}
     do
       cat ${localFilePartPathName} >> ${localTmpDir}/final/${tailDir}/${localFileName}
     done

     # Replace ^E delimiter with {^} delimiter. 
     sed -i -e "s/${destDelimiter}/{^}/g" ${localTmpDir}/final/${tailDir}/${localFileName}
     echo "Delimiter replaced to {^} for file ${localTmpDir}/final/${tailDir}/${localFileName}"  
     
     # Added {^} delimiter at the end of each line.
     sed -i -e "s/$/{^}/g" ${localTmpDir}/final/${tailDir}/${localFileName}
     echo "Added {^} at the end of each line for file ${localTmpDir}/final/${tailDir}/${localFileName}"   
  done

  # Create Dir if doesn't exists or Delete old files directory if Dir exists
  hadoop fs -mkdir -p ${brrLineItemFinalDir}/${tailDir}/
  hadoop fs -rm -r ${brrLineItemFinalDir}/${tailDir}/
  hadoop fs -mkdir -p ${brrLineItemFinalDir}/${tailDir}/
  if [ $? -ne 0 ]
  then
    echo "Script:$0 failed"
    exit 1
  fi

  # Convert/Copy local to hdfs
  hadoop fs -copyFromLocal ${localTmpDir}/final/${tailDir}/* ${brrLineItemFinalDir}/${tailDir}/
  if [ $? -ne 0 ]
  then
    echo "Script:$0 failed"
    exit 1
  fi
  echo "Merged files copied to ${brrLineItemFinalDir}/${tailDir}/"

  #List all merged files
  hadoop fs -ls ${brrLineItemFinalDir}/${tailDir}/*
done

echo "Script:$0 executed successfully"