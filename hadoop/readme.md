ENV:
- master=true true 的时候设置当前节点为 namenode，该变量必传
- slave=xxx;xxx    master 节点需配置，内容为其他节点的通讯地址
---
- master
  ```
  docker run -itd --env master=true --hostname=master --env slave=worker -p 50070:50070 -p 9000:9000 \
  -p 7077:7077 -p 8081:8080 -p 8088:8088  --name master --net ${your-network} ${your-image} 'ping localhost >> /dev/null'
  ```
- worker
   ```
   docker run -itd --env master=false --hostname=worker -p 50075:50075  --name worker --net ${your-network} ${your-image} 'ping localhost >> /dev/null'
   ```

启动完成后，spark 进入容器手动启动
```shell
/usr/local/hadoop-spark/spark/sbin/start-slave.sh
/usr/local/hadoop-spark/spark/sbin/start-master.sh
```