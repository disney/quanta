#!/bin/sh
BOOL_FLAGS=""
DATE_FILTER_FLAG=""

if [ -n "$DISTRIBUTED" -a "$DISTRIBUTED" = 'true' ] 
then
    BOOL_FLAGS="--distributed "
fi

if [ -n "$DATE_FILTER" ] 
then
    DATE_FILTER_FLAG="--date-filter $DATE_FILTER"
fi

/usr/bin/quanta-loader ${BOOL_FLAGS} ${DATE_FILTER_FLAG} --buf-size ${BUF_SIZE} ${BUCKET_PATH} ${SCHEMA_DIR} ${METADATA_DIR} ${INDEX}
