#!/bin/sh
BOOL_FLAGS=""
if [ -n "$INITIAL_POSITION" ]
then
	if [ $INITIAL_POSITION == 'TRIM_HORIZON' ]
    then
        BOOL_FLAGS="--trim-horizon"
    fi
fi
exec /usr/bin/quanta-kcl-consumer ${STREAM} ${INDEX} ${BOOL_FLAGS}
