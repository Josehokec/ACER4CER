#!/bin/bash

# run this script> ./file_split.sh nasdaq.csv 2

# check the number of arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <filename> <number_of_parts>"
    exit 1
fi

filename=$1
number_of_parts=$2

if [ ! -f "$filename" ]; then
    echo "Error: File $filename does not exist."
    exit 1
fi

total_lines=$(wc -l < "$filename")
lines_per_part=$((total_lines / number_of_parts))
remainder=$((total_lines % number_of_parts))

start_line=1
for ((i=1; i<=$number_of_parts; i++))
do
    if [ $i -le $remainder ]; then
        lines=$((lines_per_part + 1))
    else
        lines=$lines_per_part
    fi
    end_line=$((start_line + lines - 1))
    part_filename="${filename%.*}_part${i}.${filename##*.}"
    sed -n "${start_line},${end_line}p;${end_line}q" "$filename" > "$part_filename"
    start_line=$((end_line + 1))
    gzip "$part_filename"
    echo "Created and compressed $part_filename.gz"
done

echo "File $filename has been split into $number_of_parts parts and compressed."