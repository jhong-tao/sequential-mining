#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
==================================================
@Project -> File   ：sequential-mining -> prifixSpan.py
@IDE    ：PyCharm
@Author ：jhong.tao
@Date   ：2021/6/7 10:34
@Desc   ：
==================================================
"""
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.fpm import PrefixSpan
from pyspark.sql import Row
from pyspark import SparkContext

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("PrefixSpan") \
        .master("local[*]") \
        .getOrCreate()
    sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")
    df: DataFrame = sc.parallelize([Row(sequence=[[1, 2], [3]]),
                         Row(sequence=[[1], [3, 2], [1, 2]]),
                         Row(sequence=[[1, 2], [5]]),
                         Row(sequence=[[6]])]).toDF()
    df.printSchema()
    df.show()
    ticks = time.time()
    prefixSpan = PrefixSpan()
    prefixSpan.getMaxLocalProjDBSize()
    prefixSpan.getSequenceCol()
    prefixSpan.setMinSupport(0.5)
    prefixSpan.setMaxPatternLength(5)
    prefixSpan.findFrequentSequentialPatterns(df).sort("sequence").show(truncate=False)
    print(time.time()-ticks)

    spark.stop()