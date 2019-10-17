# encoding: utf-8
from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession
import traceback
import os
import hdfs
import pyspark
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, Row
import socket


def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip


os.environ['SPARK_HOME'] = "/Users/maoshuai/spark/spark"
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
df = spark.read.json("hdfs://master:9000/user/maoshuai/test/people.json")
df.printSchema()
df.show()
df.select(df.name, df.age + 1).show()
df.filter(df.age > 20).show()
df.groupBy("age").count().show()
df.sort(df.age.desc()).show()
df.sort(df.age.desc(), df.name.asc()).show()
df.select(df.name.alias("username"), df.age).show()
