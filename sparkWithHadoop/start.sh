#!/bin/sh
/usr/sbin/sshd -D & 
sleep 3;
$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh && $HADOOP_PREFIX/sbin/start-dfs.sh && $HADOOP_PREFIX/bin/hadoop fs -mkdir -p /user/root
/bin/bash -c "$*"