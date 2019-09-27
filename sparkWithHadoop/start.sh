#!/bin/sh
/usr/sbin/sshd -D &
sleep 3
>$HADOOP_PREFIX/etc/hadoop/slave
$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
if [ $(hostname -s) = "master" ]; then
  echo -e "\033[32m[namenode format start]\033[0m"
  $HADOOP_PREFIX/bin/hdfs namenode -format
  echo -e "\033[32m[master init]\033[0m"
  for i in $(echo $slave | tr ";" "\n"); do
    echo $i >>$HADOOP_PREFIX/etc/hadoop/slave
  done
  echo -e "\033[32m[slaves init successed]\033[0m"
  $HADOOP_PREFIX/sbin/start-all.sh
fi
/bin/bash -c "$*"
