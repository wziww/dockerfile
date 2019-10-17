# encoding: utf-8
from pyspark import SparkConf, StorageLevel, sql
from pyspark.sql.types import Row
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
sc = spark.sparkContext
sc.setLogLevel("OFF")
peopleRDD = sc.textFile("hdfs://master:9000/user/maoshuai/test/people.js")

schemaString = "name age"

fields = list(map(lambda fieldName: StructField(fieldName, StringType(), nullable=True), schemaString.split(" ")))

schema = StructType(fields)

rowRDD = peopleRDD.map(lambda line: line.split(',')).map(lambda attributes: Row(attributes[0], attributes[1]))
peopleDF = spark.createDataFrame(rowRDD, schema)
peopleDF.createOrReplaceTempView("people")
results = spark.sql("SELECT * FROM people")
# lists = results.collect()
results.show()
