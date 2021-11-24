#!/bin/sh
BOOL_FLAGS=""

/usr/bin/quanta-s3-kinesis-producer ${BOOL_FLAGS} --batch-size ${BATCH_SIZE} ${BUCKET_PATH} ${INDEX} ${STREAM}
