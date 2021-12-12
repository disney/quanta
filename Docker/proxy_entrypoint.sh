#!/bin/sh
BOOL_FLAGS=""
if [ -n "$LOG_LEVEL" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --log-level=${LOG_LEVEL}"
fi
if [ -n "$ENV" ]
then
    BOOL_FLAGS=${BOOL_FLAGS}" --env=${ENV}"
fi
USER_KEY_FLAG=""
if [ -n "$USER_KEY" ]
then
    USER_KEY_FLAG="--user-key $USER_KEY"
fi
exec /usr/bin/quanta-proxy ${PUBLIC_KEY_URL} ${USER_KEY_FLAG} ${BOOL_FLAGS}
