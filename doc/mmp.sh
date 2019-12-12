#!/usr/bin/env bash

export HADOOP_USER_NAME=hdfs
#cd /bigdata/develop/aston/bzh_project
cd /home/logoper/mmp

hbase_client=/usr/hdp/current/hbase-client/lib


#nohup /usr/hdp/current/spark2-client/bin/spark-submit --class com.bsb.mmp.demo.MonitorJob --master yarn --deploy-mode cluster --driver-memory 1g --executor-memory 4g --executor-cores 2 --queue default --jars $hbase_client/hbase-client-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-server-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-common-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-protocol-1.1.2.2.6.0.3-8.jar scala-1.0-SNAPSHOT.jar
#/usr/hdp/current/spark2-client/bin/spark-submit --class com.bsb.mmp.demo.MonitorJob --master local[*] --driver-memory 1g --executor-memory 4g --executor-cores 2 --queue default --jars $hbase_client/hbase-client-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-server-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-common-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-protocol-1.1.2.2.6.0.3-8.jar scala-1.0-SNAPSHOT.jar
nohup /usr/hdp/current/spark2-client/bin/spark-submit --class com.bsb.mmp.demo.MonitorJob --master yarn --deploy-mode client --driver-memory 1g --executor-memory 4g --executor-cores 2 --queue default --jars $hbase_client/hbase-client-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-server-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-common-1.1.2.2.6.0.3-8.jar,$hbase_client/hbase-protocol-1.1.2.2.6.0.3-8.jar scala-1.0-SNAPSHOT.jar
