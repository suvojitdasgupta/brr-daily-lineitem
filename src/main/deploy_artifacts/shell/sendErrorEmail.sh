#!/bin/bash

export AWS_DEFAULT_REGION="us-east-1"
property_file_name="job_env.properties"
encrypted_property_file_name="${property_file_name}.encrypted"

function get_env_specific_property_file_from_hdfs(){
	hadoop fs -copyToLocal ${confS3Path}/* ./
	if [ $? -ne 0 ]; then
		echo -e "\n\n Error getting the ${confS3Path}/${property_file_name}* from HDFS . Exiting ! \n\n" >&2
		exit 1
	fi
	
	if [ -f ./${property_file_name} -a -s ./${property_file_name} ]
	then
		echo " Un-encrypted ${property_file_name} found"
		source ./${property_file_name}
		
	elif [ -f ./${encrypted_property_file_name} -a -s ./${encrypted_property_file_name} ]
	then
		echo " Encrypted_property_file=${encrypted_property_file_name} found. Decrypting the file"
		aws kms decrypt --ciphertext-blob fileb://${encrypted_property_file_name} --output text --query Plaintext | base64 --decode > ./${property_file_name}
		source ./${property_file_name}
		rm ./${property_file_name}
	else
		echo "${property_file_name} or ${encrypted_property_file_name} not found. Exiting"
		exit 1
	fi
}

function ensure_variables_are_set(){

	if [ -z "confS3Path" -o -z "$segmentDate" -o -z "$reportDate" -o -z "$workflowId" -o -z "$errorMessage" -o -z "$environment" -o -z "$errorEmailToAddress" ];then
		echo -e "\n\nArgument(s) not set ! confS3Path=${confS3Path} , segmentDate=${segmentDate} , reportDate=${reportDate} , workflowId=${workflowId} , errorMessage=${errorMessage} , environment=${environment} , errorEmailToAddress=${errorEmailToAddress} . Exiting.."
		exit 1
	fi
}

function update_emailbody_with_tracker_url(){
	local jobflow_json_path="/mnt/var/lib/info/job-flow.json"
	local instance_json_path="/mnt/var/lib/info/instance.json"
	if [ -f ${instance_json_path} -a -s ${instance_json_path} -a -f ${jobflow_json_path} -a -s ${jobflow_json_path} ]
	then
		local cluster_id=$(echo `jq -r '.jobFlowId' ${jobflow_json_path}`)
		echo "cluster_id : ${cluster_id}"
		if [ ! -z "${cluster_id}" ]
		then
			local master_node_private_dns="$(echo `aws emr list-instances --cluster-id "$cluster_id" --instance-group-types "MASTER"|jq -r '.Instances|.[0].PrivateDnsName'`)"
			echo "master_node_private_dns : ${master_node_private_dns}"
			if [ ! -z "${master_node_private_dns}" ]
			then
				local oozie_wf_url="http://${master_node_private_dns}:11000/oozie/?job=${workflowId}"
				echo "oozie_wf_url : ${oozie_wf_url}"			
				errorEmailBody="${errorEmailBody} \n oozie_wf_url = ${oozie_wf_url}"
			fi
		fi
	else
		echo " Files ${instance_json_path} , ${jobflow_json_path} not present. Skipping to update oozie_wf_url"
	fi
}
function send_error_mail(){
	echo -e "${errorEmailBody}" | /bin/mail -s "$errorEmailSubject" "${errorEmailToAddress}"
}

confS3Path=$1
segmentDate=$2
reportDate=$3
workflowId=$4
errorMessage=$5
environment=$6
errorEmailSubject="Subject: ${environment} - BRR Hadoop Lineitem Failure for ${reportDate} "
errorEmailBody=" segmentDate=${segmentDate} \n reportDate=${reportDate} \n workflowId=${workflowId} \n errorMessage=${errorMessage} \n" 

get_env_specific_property_file_from_hdfs || exit 1
ensure_variables_are_set || exit 1

update_emailbody_with_tracker_url


echo -e "\n\n=========== SendErrorEmail - Begin ===========\n\n"

echo -e "Input params, "
echo -e "confS3Path: ${confS3Path}"
echo -e "segmentDate: ${segmentDate}"
echo -e "reportDate: ${reportDate}"
echo -e "workflowId: ${workflowId}"
echo -e "errorMessage: ${errorMessage}"
echo -e "environment: ${environment}"
echo -e "errorEmailToAddress: ${errorEmailToAddress}"
echo -e "errorEmailSubject: ${errorEmailSubject}"
echo -e "errorEmailBody: ${errorEmailBody}"

send_error_mail || exit 1

echo -e "\n\n=========== SendErrorEmail - Complete ===========\n\n"