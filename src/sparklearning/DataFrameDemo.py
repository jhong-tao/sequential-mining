#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
==================================================
@Project -> File   ：sequential-mining -> DataFrameDemo.py
@IDE    ：PyCharm
@Author ：jhong.tao
@Date   ：2021/6/7 10:34
@Desc   ：
==================================================
"""
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName('DataFrameDemo')\
        .master('local')\
        .getOrCreate()

    words_df = spark.read.text("E:\git\python\sequential-mining\data\input\wordCount.txt")
    json_df = spark.read.json('E:\git\python\sequential-mining\data\input\json.json')
    csv_df = spark.read.csv("E:\git\python\sequential-mining\data\input\csv.csv")

    words_df.printSchema()
    words_df.show()

    json_df.printSchema()
    json_df.show()

    csv_df.printSchema()
    csv_df.show()

    spark.stop()