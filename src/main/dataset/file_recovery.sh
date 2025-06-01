#!/bin/bash

# run this script> ./file_recovery.sh nasdaq
# run this script> ./file_recovery.sh job
# run this script> ./file_recovery.sh crimes

# check the number of arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 <filename_prefix> (without .csv) "
    exit 1
fi

# obtain prefix of filename
filename_prefix=$1
output_file="${filename_prefix}.csv"

# decompress
for file in "${filename_prefix}_part"*.gz; do
    # check file then decompress
    if [ -f "$file" ]; then
        gunzip -c "$file" >> "$output_file"
        echo "Decompressed and appended $file to $output_file"
    fi
done

echo "All parts have been decompressed and merged into $output_file."