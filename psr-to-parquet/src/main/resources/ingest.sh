#!/bin/bash
filename='pstFiles'
exec 4<$filename
echo Start
while read -u4 p ; do
    echo $p
    /home/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --master local[4] --driver-memory 12g --class com.hugolinton.IngestData /home/ec2-user/hugo-linton-test-1.0-SNAPSHOT.jar $p /data/parquet/ 
done
