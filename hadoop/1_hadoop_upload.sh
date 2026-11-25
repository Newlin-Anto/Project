#!/bin/bash
# STEP 1 - HADOOP: copy the CSV from your normal disk into HDFS.
# Run from inside the "payment" folder:   ./1_hadoop_upload.sh

set -e

# This is the HDFS folder name (you can change "payment" if you want)
HDFS_DIR=/payment

echo "HDFS address : $(hdfs getconf -confKey fs.defaultFS)"

hdfs dfs -mkdir -p $HDFS_DIR                         # create folder in HDFS
hdfs dfs -put -f payment_gateway_raw.csv $HDFS_DIR/  # copy file local -> HDFS

echo "---- Files now inside HDFS folder $HDFS_DIR ----"
hdfs dfs -ls -h $HDFS_DIR
echo "Done. You can also see it at http://localhost:9870 (Utilities -> Browse file system)"
