#!/bin/sh
#############################################################################################################################
##
##     Script Name : LI_hdfs_cleanup.sh
##         Purpose : The purpose of the script is to cleanup all old data files from previous runs in order to maintain 
##                   60% free disk space for current runs.
##           Usage : ./LI_hdfs_cleanup.sh <brrLineItemAppDir>
##
#############################################################################################################################

# Main
# Input Parameters
TARGET_DIR=$1
echo "Target Directory for cleanup is: ${TARGET_DIR}"

# Check for valid directory
hadoop fs -test -e "${TARGET_DIR}/data/"
if [ $? -eq 0 ]
then
  echo "${TARGET_DIR}/data/ is present. Continue"
else
  echo "${TARGET_DIR}/data/ not present. No action needed. Skipping the cleanup activity."
  exit 0
fi

# Cleanup
while [[ `hadoop fs -test -e "${TARGET_DIR}/data/"` -eq 0 && `hadoop fs -df -h ${TARGET_DIR}/data/ | tail -1 | awk -F " " '{print int($8)}'` -gt 60 ]]
do
      AVAILAIBLE_SPACE_IN_GB=$(hadoop fs -df -h ${TARGET_DIR}/data/ | tail -1 | awk -F " " '{print int($6)}') 2>/dev/null
      AVAILAIBLE_SPACE_IN_PERCENT=$(hadoop fs -df -h ${TARGET_DIR}/data/ | tail -1 | awk -F " " '{print int($8)}') 2>/dev/null
      
      echo "Available disk space is (Threshold is 40% Free): ${AVAILAIBLE_SPACE_IN_GB}(${AVAILAIBLE_SPACE_IN_PERCENT}% Used). Actions needed."

      OLDEST_DIR=$(hadoop fs -ls -d ${TARGET_DIR}/data/*/*/* 2>/dev/null | sort | head -1 | awk '{print $NF}' )
      echo "Oldest Directory for cleanup is: $OLDEST_DIR"
      
      hadoop fs -rm -r ${OLDEST_DIR} 2>/dev/null
done