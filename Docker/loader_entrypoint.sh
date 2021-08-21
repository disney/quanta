#!/bin/sh
BOOL_FLAGS=""
DATE_FILTER_FLAG=""

if [ -n "$DATE_FILTER" ] 
then
    DATE_FILTER_FLAG="--date-filter $DATE_FILTER"
fi

/usr/bin/quanta-loader ${BOOL_FLAGS} ${DATE_FILTER_FLAG} --buf-size ${BUF_SIZE} ${BUCKET_PATH} ${SCHEMA_DIR} ${INDEX}
