#!/bin/sh
#####################################################################################################################################################
##                                                                                                                                                  #
##     Script Name : LI_s3_to_local_copy.sh                                                                                                         #
##         Purpose : The purpose of the script is to copy the following AWS S3 files to respective locations on Local disk.                         #
##                                                                                                                                                  #
##                                                          Source        Target                                                                    #
##    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
##    OBI Extracts and DVT:                                                                                                                         #
##           $S3_DATA_DIR/final/output/LI_15_process_dvt_metrics_done  -->  $BRROBI_OUTPUT_DIR/BRR_OBI_lineitem_extract.$RUN_DT.$RUN_DT.done        #
##            $S3_DATA_DIR/final/output/LI_15_process_dvt_metrics_dvt  -->  $BRROBI_OUTPUT_DIR/BRR_OBI_lineitem_extract.$RUN_DT.$RUN_DT.out         #
##            $S3_DATA_DIR/final/output/LI_15_process_dvt_metrics_dvt  -->  $DVTOBI_STAGING/BRR_OBI_lineitem_extract.$RUN_DT.$RUN_DT.out            #
##         $S3_DATA_DIR/final/output/LI_15_process_dvt_metrics_report  -->  $BRROBI_OUTPUT_DIR/BRR_OBI_lineitem_extract.$RUN_DT.$RUN_DT.report      #
##                                                                                                                                                  #
##   BRR:                                                                                                                                           #
##                  ${S3_DATA_DIR}/final/output/LI_16_process_brr_exp  -->  ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail_exp.${RUN_DT}.out         #
##                  $S3_DATA_DIR/final/output/LI_16_process_brr_merge  -->  $BRROBI_UPLOAD_DIR/BRR_OBI_line_item_detail.$RUN_DT.out                 #
##                                                                                                                                                  #
##   Errors and Rejects:                                                                                                                            #
##       $S3_DATA_DIR/final/rejects/LI_18_process_brr_rejects_recycle  -->  $BRROBI_RECYCLE_DIR/BRR_OBI_line_item_detail.$RUN_DT.rec                #
##       $S3_DATA_DIR/final/rejects/LI_18_process_brr_rejects_rejects  -->  $BRROBI_REJECT_DIR/BRR_OBI_line_item_detail.$RUN_DT.rej                 #
##  $S3_DATA_DIR/final/rejects/LI_18_process_brr_rejects_lookup_error  -->  $BRROBI_REJECT_DIR/BRR_OBI_line_item_detail_lkup_err.$RUN_DT.error      #
##                                                                                                                                                  #
##           Usage : ./LI_s3_to_local_copy.sh <date,YYYY-MM-DD,optional>  <ENV, dev|qa|prod>                                                        #
##                                                                                                                                                  #
#####################################################################################################################################################

# Function
function error_check {
  if [ $? -eq 0 ]
  then
    echo "Copy successful from $1 to $2"
  else
    echo "Copy failed from $1 to $2"
    exit 1
  fi
}

# Proxy Settings
# export HTTPS_PROXY=http://proxy.dev.obi.aol.com
# export HTTP_PROXY=http://proxy.dev.obi.aol.com

# Main
# Input Parameters
RUN_DT=${1:-`date -u +%Y-%m-%d -d "1 day ago"`}
ENV=$2

echo "Script:$0 started"
echo "Run Date: $RUN_DT"
echo "ENV : $ENV"

YEAR=$(date -d "$RUN_DT"  '+%Y')
MONTH=$(date -d "$RUN_DT" '+%m')
DAY=$(date -d "$RUN_DT" '+%d')

BRROBI_OUTPUT_DIR=/data/staging/brrOBI/output
DVTOBI_STAGING=/data/staging/dvtOBI/dvtStaging
BRROBI_UPLOAD_DIR=/data/staging/brrOBI/upload
BRROBI_RECYCLE_DIR=/data/staging/brrOBI/recycle/auto
BRROBI_REJECT_DIR=/data/staging/brrOBI/recycle/manual

S3_DATA_DIR=s3://obi-brr-data-bucket-${ENV}/brr-daily-lineitem/${YEAR}/${MONTH}/${DAY}
S3_STATUS_DIR=s3://obi-brr-status-bucket-${ENV}/daily_jobs/brr-daily-lineitem/${YEAR}/${MONTH}/${DAY}

#Wait loop
AWS_JOB_STATUS=""
while [ "$AWS_JOB_STATUS" != "DONE" ]
do
   echo " Inside Wait loop ... Waiting for the DONE file"
   sleep 20
   AWS_JOB_STATUS=`aws s3 ls ${S3_STATUS_DIR}/DONE | awk '{print $NF}'`
   echo "AWS_JOB_STATUS : $AWS_JOB_STATUS"
done
echo "Encountered DONE file. Exiting wait loop"

# Delete existing files before copy
echo "Initiating target location cleanup"
rm -f ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.done 2>>/dev/null
rm -f ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.out 2>>/dev/null
rm -f ${DVTOBI_STAGING}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.out 2>>/dev/null
rm -f ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.report 2>>/dev/null
rm -f ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail_exp.${RUN_DT}.out 2>>/dev/null
rm -f ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.out 2>>/dev/null
rm -f ${BRROBI_RECYCLE_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.rec 2>>/dev/null
rm -f ${BRROBI_REJECT_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.rej 2>>/dev/null
rm -f ${BRROBI_REJECT_DIR}/BRR_OBI_line_item_detail_lkup_err.${RUN_DT}.error 2>>/dev/null
echo "Target location cleanup complete"

# Copy
echo "Initiating copy"

aws s3 cp ${S3_DATA_DIR}/final/output/LI_15_process_dvt_metrics_done ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.done
error_check ${S3_DATA_DIR}/final/output/LI_15_process_dvt_metrics_done ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.done

aws s3 cp ${S3_DATA_DIR}/final/output/LI_15_process_dvt_metrics_dvt ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.out
error_check ${S3_DATA_DIR}/final/output/LI_15_process_dvt_metrics_dvt ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.out

aws s3 cp ${S3_DATA_DIR}/final/output/LI_15_process_dvt_metrics_dvt ${DVTOBI_STAGING}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.out
error_check ${S3_DATA_DIR}/final/output/LI_15_process_dvt_metrics_dvt ${DVTOBI_STAGING}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.out

aws s3 cp ${S3_DATA_DIR}/final/output/LI_15_process_dvt_metrics_report ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.report
error_check ${S3_DATA_DIR}/final/output/LI_15_process_dvt_metrics_report ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.report

aws s3 cp ${S3_DATA_DIR}/final/output/LI_16_process_brr_exp ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail_exp.${RUN_DT}.out
error_check ${S3_DATA_DIR}/final/output/LI_16_process_brr_exp ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail_exp.${RUN_DT}.out

aws s3 cp ${S3_DATA_DIR}/final/output/LI_16_process_brr_merge ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.out
error_check ${S3_DATA_DIR}/final/output/LI_16_process_brr_merge ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.out

aws s3 cp ${S3_DATA_DIR}/final/output/LI_18_process_brr_rejects_recycle ${BRROBI_RECYCLE_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.rec
error_check ${S3_DATA_DIR}/final/output/LI_18_process_brr_rejects_recycle ${BRROBI_RECYCLE_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.rec

aws s3 cp ${S3_DATA_DIR}/final/rejects/LI_18_process_brr_rejects_rejects ${BRROBI_REJECT_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.rej
error_check ${S3_DATA_DIR}/final/rejects/LI_18_process_brr_rejects_rejects ${BRROBI_REJECT_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.rej

aws s3 cp ${S3_DATA_DIR}/final/rejects/LI_18_process_brr_rejects_lookup_error ${BRROBI_REJECT_DIR}/BRR_OBI_line_item_detail_lkup_err.${RUN_DT}.error
error_check ${S3_DATA_DIR}/final/rejects/LI_18_process_brr_rejects_lookup_error ${BRROBI_REJECT_DIR}/BRR_OBI_line_item_detail_lkup_err.${RUN_DT}.error

echo "Copy completed"

chmod 755 ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.done 2>>/dev/null
chmod 755 ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.out 2>>/dev/null
chmod 755 ${DVTOBI_STAGING}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.out 2>>/dev/null
chmod 755 ${BRROBI_OUTPUT_DIR}/BRR_OBI_lineitem_extract.${RUN_DT}.${RUN_DT}.report 2>>/dev/null
chmod 755 ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail_exp.${RUN_DT}.out 2>>/dev/null
chmod 755 ${BRROBI_UPLOAD_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.out 2>>/dev/null
chmod 755 ${BRROBI_RECYCLE_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.rec 2>>/dev/null
chmod 755 {BRROBI_REJECT_DIR}/BRR_OBI_line_item_detail.${RUN_DT}.rej 2>>/dev/null
chmod 755 ${BRROBI_REJECT_DIR}/BRR_OBI_line_item_detail_lkup_err.${RUN_DT}.error 2>>/dev/null

echo "File permisssions modified to 755"

echo "Script:$0 finished"
