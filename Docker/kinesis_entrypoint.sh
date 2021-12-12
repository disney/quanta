#!/bin/sh
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
if [ -n "$LOG_LEVEL" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --log-level=${LOG_LEVEL}"
fi
if [ -n "$ENV" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --env=${ENV}"
fi
exec /usr/bin/quanta-kinesis-consumer ${STREAM} ${INDEX} ${ASSUME_ROLE_ARN} us-east-2 ${BOOL_FLAGS}
