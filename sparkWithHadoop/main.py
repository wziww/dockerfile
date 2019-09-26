# from __future__ import print_function
# from pyspark import SparkContext, SparkSession
# import hdfs

# client = hdfs.Client("http://10.0.3.150:50070")

# lists = client.list("/ms/")
# for f in lists:
#     print(f)

# spark = SparkSession \
#     .builder \
#     .enableHiveSupport() \
#     .master("10.0.3.150:7077") \
#     .appName("my_first_app_name") \
#     .getOrCreate()
# # sc = SparkContext(master='spark://10.0.3.150:7077', appName='test')
# spark
# textFile = sc.textFile("hdfs:///ms/words.txt")

# wordCount = textFile.flatMap(lambda line: line.split(" ")).map(
#     lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# wordCount.foreach(print)
