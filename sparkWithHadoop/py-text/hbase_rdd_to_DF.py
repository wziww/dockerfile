# encoding: utf-8
from pyspark import SparkConf, StorageLevel, sql
from pyspark.sql.types import Row
from pyspark.sql import SparkSession
import traceback
import os
import hdfs
import json
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

sc.setLogLevel("ERROR")
host = 'master'
table = 'student'
conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                               "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                               "org.apache.hadoop.hbase.client.Result", keyConverter=keyConv, valueConverter=valueConv,
                               conf=conf)


def call_transfor(y1):
    y2 = [json.loads(i) for i in y1]
    fdc = {}
    for i in y2:
        colname = i['qualifier']
        value = i['value']
        fdc[colname] = value

    return fdc


# 將hbase RDD轉換為DataFrame

def rdd_to_df(hbase_rdd):
    # 同一個RowKey對應的列之間是用\n分割，進行split，split後每列是個dict
    fdc_split = hbase_rdd.map(lambda x: (x[0], x[1].split('\n')))
    # 提取列名和取值
    fdc_cols = fdc_split.map(lambda x: (x[0], call_transfor(x[1])))
    colnames = ['row_key'] + fdc_cols.map(lambda x: [i for i in x[1]]).take(1)[0]
    print colnames
    fdc_dataframe = fdc_cols.map(lambda x: [x[0]] + [x[1][i] for i in x[1]]).toDF(colnames)
    return fdc_dataframe


# 資料轉換

fdc_data = rdd_to_df(hbase_rdd)
fdc_data.show()
