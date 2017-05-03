#!/usr/bin/env bash

/home/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --master local[4] --driver-memory 12g --class com.hugolinton.query.ParquetQuery /home/ec2-user/hugo-linton-test-1.0-SNAPSHOT.jar /data/parquet/