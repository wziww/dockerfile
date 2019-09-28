from pyspark import SparkConf
from pyspark.sql import SparkSession
import traceback
import os
appname = "test"  # 任务名称
master = "spark://10.0.3.150:7077"  # "spark://host:port"
'''
pyspark 远程 hdfs 文件词频统计
'''
spark_driver_host = "10.0.2.32"  # 本地主机ip
conf = SparkConf().setAppName(appname).setMaster(
    master).set("spark.driver.host", spark_driver_host).setExecutorEnv("spark.executor.memory", "512m")
spark = SparkSession.builder.config(conf=conf) \
    .config("spark.cores.max", "1") \
    .config('spark.executor.memory', '512m') \
    .getOrCreate()
sc = spark.sparkContext
textFile = sc.textFile("hdfs://10.0.3.150:9000/ms/test/*")

wordCount = textFile.flatMap(lambda line: line.split(" ")).map(
    lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)


def f(iterator):
    for x in iterator:
        print(x[0])


result = wordCount.collect()
f(result)
