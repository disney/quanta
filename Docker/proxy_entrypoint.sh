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
    USER_KEY_FLAG="--user-key=${USER_KEY}"
fi
SESSION_POOL_SIZE_FLAG=""
if [ -n "$SESSION_POOL_SIZE" ]
then
    SESSION_POOL_SIZE_FLAG="--pool-size=${SESSION_POOL_SIZE}"
fi
exec /usr/bin/quanta-proxy ${PUBLIC_KEY_URL} ${REGION} ${USER_KEY_FLAG} ${SESSION_POOL_SIZE_FLAG} ${BOOL_FLAGS}
