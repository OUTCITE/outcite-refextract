#!/bin/bash -eu

DIRECTORY_TO_OBSERVE="/home/outcite/deployment/refextract/pdfs/UPLOADS/"
BUILD_SCRIPT=code/M_USERS.sh
REPEAT_INTERVAL=10 # seconds

function block_for_change {
  inotifywait --timeout $REPEAT_INTERVAL \
    --event modify,move,create,delete \
    $DIRECTORY_TO_OBSERVE
}

while true; do
    block_for_change |
    while read -r dir action file; do
        echo "The file '$file' appeared in directory '$dir' via '$action'"
        sum1="$(md5sum "${dir}${file}")"
        sleep 1
        sum2="$(md5sum "${dir}${file}")"
        while [ "$sum1" != "$sum2" ]; do
            sum1="$(md5sum "${dir}${file}")"
            sleep 1
            sum2="$(md5sum "${dir}${file}")"
        done;
        bash $BUILD_SCRIPT
        wait;
    done;
    bash $BUILD_SCRIPT
done
