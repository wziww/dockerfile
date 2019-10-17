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
textFile = sc.textFile("hdfs://10.0.3.150:9000/user/maoshuai/wordCount.txt")
# use rdd cache
textFile.persist(StorageLevel.MEMORY_AND_DISK_2)

wordCount = textFile.flatMap(lambda line: line.split(" ")).map(
    lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False)
lists = wordCount.collect()
print lists
