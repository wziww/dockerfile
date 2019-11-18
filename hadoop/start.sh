#!/bin/sh
/usr/sbin/sshd -D &
sleep 3
>$HADOOP_PREFIX/etc/hadoop/slaves
$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
# $HBASE_PREFIX/conf/hbase-env.sh
# such as $slave=worker01;worker02 then they will be set into slaves file
# master = true to set master
if [ $master = "true" ]; then
  echo -e "\033[32m[master init]\033[0m"
  for i in $(echo $slave | tr ";" "\n"); do
    echo $i >>$HADOOP_PREFIX/etc/hadoop/slaves
  done
  echo -e "\033[32m[slaves init successed]\033[0m"
  $HADOOP_PREFIX/sbin/start-all.sh
  # $HBASE_PREFIX/bin/start-hbase.sh
fi
/bin/bash -c "$*"
