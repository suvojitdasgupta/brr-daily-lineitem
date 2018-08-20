#!/bin/sh

export HADOOP_USER_NAME="hadoop"
exec_command="pig -Dpig.additional.jars=brr-daily-lineitem.jar $@ || exit 1"
echo -e "Pig script execution - Begin" 
echo -e "Executing the pig script with the following command as `whoami` user,\n ${exec_command}" 
`$exec_command`||exit 1
echo -e "Pig script execution - Completed Successfully" 