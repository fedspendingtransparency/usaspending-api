#!/bin/bash
FILE=$1
EXIT_CODE=0

while read line ; do \
    echo "$line" | grep "warn"
    if [ $? = 0 ]; then
        EXIT_CODE=$(($EXIT_CODE + 1))
    fi
done <<< "$(cat $FILE)"

exit $EXIT_CODE
