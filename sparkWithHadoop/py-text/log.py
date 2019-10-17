# encoding: utf-8
from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession
import traceback
import os
import hdfs
import pyspark
import socket


def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip


hdfsClient = hdfs.Client("http://10.0.3.150:50070")
appname = "test"  # 任务名称
master = "spark://10.0.3.150:7077"  # "spark://host:port"
'''
pyspark 远程 hdfs 文件词频统计
'''
spark_driver_host = get_host_ip()  # 本地主机ip
conf = SparkConf().setAppName(appname).setMaster(
    master).set("spark.driver.host", spark_driver_host).setExecutorEnv("spark.executor.memory", "512m")  # \
spark = SparkSession.builder \
    .config(conf=conf) \
    .config("spark.cores.max", "1") \
    .config('spark.executor.memory', '512m') \
    .getOrCreate()
sc = spark.sparkContext
textFile = sc.textFile("hdfs://10.0.3.150:9000/user/maoshuai/logs/*.log")
# use rdd cache
textFile.persist(StorageLevel.MEMORY_AND_DISK_2)


# 140.250.75.163 - - [14/Oct/2019:00:27:53 +0800]
# "GET http://pic.yupoo.com/fotomag/BvyWwFI6/t7b7O.jpg HTTP/1.1" 200 72728
# "-" "CLKit 1.0 rv:1 (iPhone; iOS 12.3.1; en_CN)" "image/jpeg" 0 Hit "U/200"
# Static "max-age=691200" 0.041 150.138.88.20
def parse(line):
    request = ""
    referer = ""
    quotes = 0
    last_quote = 0
    chars = list(line)
    for i in range(len(chars)):
        if ord(chars[i]) == 34:
            quotes += 1
            if quotes == 2:
                request = line[last_quote + 1:i]
            elif quotes == 4:
                referer = line[last_quote + 1:i]
            last_quote = i
        if quotes >= 4:
            break
    if request == "" or referer == "":
        return None
    request_array = request.split('/')
    value_array = line.split(' ')
    if len(request_array) > 4 and len(value_array) > 10:
        return request_array[3], int(value_array[9])
    return None


wordCount = textFile.map(lambda x: parse(x)). \
    filter(lambda x: x is not None) \
    .map(
    lambda word: (word[0], word[1])). \
    reduceByKey(lambda a, b: a + b). \
    sortBy(lambda x: x[1], False)
results = wordCount.collect()
for v in results:
    print v
# wordCount.saveAsTextFile("hdfs://10.0.3.150:9000/user/maoshuai/logs/output")
