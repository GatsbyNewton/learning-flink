#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: sh ./$0 <source file path>"
    exit -1
fi

for line in `cat $1`; do
    echo "$line"
    sleep 0.001
done