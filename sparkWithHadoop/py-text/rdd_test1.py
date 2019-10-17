from pyspark import SparkConf
from pyspark.sql import SparkSession
import traceback
import os
'''
pyspark 数量统计
'''
appname = "test"  # 任务名称
master = "spark://10.0.3.150:7077"  # "spark://host:port"
spark_driver_host = "10.0.2.32"  # 本地主机ip
try:
    conf = SparkConf().setAppName(appname).setMaster(
        master).set("spark.driver.host", spark_driver_host).setExecutorEnv("spark.executor.memory", "512m")
    spark = SparkSession.builder.config(conf=conf) \
        .config("spark.cores.max", "1") \
        .config('spark.executor.memory', '512m') \
        .getOrCreate()
    sc = spark.sparkContext
    words = sc.parallelize(
        ["scala",
         "java",
         "hadoop",
         "spark",
         "akka",
         "spark vs hadoop",
         "pyspark",
         "pyspark and spark"
         ])

    def f(x):
        print(x)
    words.foreach(f)
    counts = words.count()
    sc.stop()
    # sc.parallelize([1, 2, 3, 4, 5]).foreach(f)
    print("Number of elements in RDD is %i" % counts)

    print('计算成功！')
except:
    sc.stop()
    traceback.print_exc()  # 返回出错信息
    print('连接出错！')
