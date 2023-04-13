#!/bin/sh
BOOL_FLAGS=""
if [ -n "$IGNORE_SOURCE_PATH" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --ignore-source-path"
fi
if [ -n "$NERD_CAPITALIZATION" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --nerd-capitalization"
fi
if [ -n "$ROLE_ARN" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --role-arn=${ROLE_ARN}"
fi
if [ -n "$AWS_REGION" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --aws-region=${AWS_REGION}"
fi
if [ -n "$KMS_KEY_ID" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --kms-key-id=${KMS_KEY_ID}"
fi
if [ -n "$ACL" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --acl=${ACL}"
fi
/usr/bin/quanta-loader ${BUCKET_PATH} ${INDEX} --buf-size ${BUF_SIZE} ${BOOL_FLAGS}
