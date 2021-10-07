#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
==================================================
@Project -> File   ：sequential-mining -> wordcount.py
@IDE    ：PyCharm
@Author ：jhong.tao
@Date   ：2021/6/7 10:34
@Desc   ：
==================================================
"""
# coding = utf-8
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # 输入的数据
    lines_rdd = sc.textFile("../data/input/wordCount.txt")

    input_rdd = lines_rdd.flatMap(lambda x: x.split(" "))

    resultRdd = input_rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

    resultColl = resultRdd.collect()
    for line in resultColl:
        print(line)

    resultRdd.repartition(numPartitions=1).saveAsTextFile(path="../data/output/resultWordcount")

    sc.stop()
    
