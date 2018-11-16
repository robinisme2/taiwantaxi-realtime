#!/bin/bash
keep=50
for d in /tmp/spark-streaming/*/; do
    if [ "$(ls -A $d)" ]; then
        ls -t -d $d/* | tail -n "+$(($keep + 1))" | xargs rm -rf
    fi
done
