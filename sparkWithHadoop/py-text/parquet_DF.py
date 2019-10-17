# encoding: utf-8
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
import socket
import __future__


# /usr/local/hadoop-spark/spark/examples/src/main/resources/users.parquet
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
spark_driver_host = get_host_ip()  # 本地主机ip
conf = SparkConf().setAppName(appname).setMaster(
    master).set("spark.driver.host", spark_driver_host)
spark = SparkSession.builder \
    .config(conf=conf) \
    .config("spark.cores.max", "2") \
    .config('spark.executor.memory', '2g') \
    .getOrCreate()

pdf = spark.read.parquet("hdfs://master:9000/user/maoshuai/users.parquet")
pdf.createOrReplaceTempView("parquetFile")
namesdf = spark.sql("SELECT * FROM parquetFile")


def printnames(x):
    print x;
    return


pdf.rdd.foreach(lambda person: printnames(person))
