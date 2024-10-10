#!/bin/sh

if [ -z "$SCHEMA" ]
then
	SCHEMA="quanta"
fi

BOOL_FLAGS=""
if [ -n "$INITIAL_POSITION" ]
then
    if [ $INITIAL_POSITION == 'TRIM_HORIZON' ]
    then
        BOOL_FLAGS="--trim-horizon"
    fi
fi
if [ -n "$NO_CHECKPOINTER" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --no-checkpoint-db"
fi
if [ -n "$CHECKPOINT_TABLE" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --checkpoint-table=${CHECKPOINT_TABLE}"
fi
if [ -n "$LOG_LEVEL" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --log-level=${LOG_LEVEL}"
fi
if [ -n "$ENV" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --env=${ENV}"
fi
if [ -n "$DEAGGREGATE" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --deaggregate"
fi
if [ -n "$AVRO" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --avro-payload"
fi
if [ -n "$ASSUME_ROLE_ARN" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --assume-role-arn=${ASSUME_ROLE_ARN}"
	if [ -n "$ASSUME_ROLE_ARN_REGION" ]
	then
    	BOOL_FLAGS=${BOOL_FLAGS}" --assume-role-arn-region=${ASSUME_ROLE_ARN_REGION}"
	fi
fi
if [ -n "$SCAN_INTERVAL" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --scan-interval="${SCAN_INTERVAL}""
fi
if [ -n "$PROTO_PATH" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --proto-path=${PROTO_PATH}"
fi
exec /usr/bin/quanta-kinesis-consumer ${STREAM} ${SCHEMA} ${SHARD_KEY} ${REGION} ${BOOL_FLAGS}
