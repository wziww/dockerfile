#!/bin/sh
/usr/sbin/sshd -D &
sleep 3
>$HADOOP_PREFIX/etc/hadoop/slave
$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
$HADOOP_PREFIX/bin/hdfs namenode -format
if [ $(hostname -s) = "master" ]; then
  for i in $(echo $slave | tr ";" "\n"); do
    $i >>$HADOOP_PREFIX/etc/hadoop/slave
  done
  $HADOOP_PREFIX/sbin/start-all.sh
fi
/bin/bash -c "$*"
