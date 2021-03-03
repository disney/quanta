#!/bin/sh
USER_KEY_FLAG=""
if [ -n "$USER_KEY" ]
then
    USER_KEY_FLAG="--user-key $USER_KEY"
fi
exec /usr/bin/quanta-proxy ${SCHEMA_DIR} ${METADATA_DIR} ${PUBLIC_KEY_URL} --logging debug ${USER_KEY_FLAG}
