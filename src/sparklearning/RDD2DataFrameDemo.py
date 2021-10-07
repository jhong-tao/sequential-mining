#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
==================================================
@Project -> File   ：sequential-mining -> RDD2DataFrameDemo.py
@IDE    ：PyCharm
@Author ：jhong.tao
@Date   ：2021/6/7 10:34
@Desc   ：
==================================================
"""
from pyspark.sql import SparkSession
from pyspark.rdd import RDD
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    spark = SparkSession.builder.appName("RDD2DataFrameDemo").master("local").getOrCreate()
    sc: SparkContext = spark.sparkContext
    lines_rdd = sc.textFile("E:\git\python\sequential-mining\data\input\person.txt")
    person_rdd = lines_rdd.map(lambda x: x.split(" "))
    print(person_rdd.collect())

    person_df = spark.createDataFrame(person_rdd, schema=['id', 'name', 'age'])

    person_df.printSchema()
    person_df.show()

    spark.stop()

