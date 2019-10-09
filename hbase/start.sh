#!/bin/sh
/usr/sbin/sshd -D &
sleep 3
$HBASE_PREFIX/conf/hbase-env.sh
$HBASE_PREFIX/bin/start-hbase.sh
/bin/bash -c "$*"
