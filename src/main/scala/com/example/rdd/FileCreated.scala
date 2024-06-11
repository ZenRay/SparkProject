package com.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FileCreated {
    def main(args: Array[String]): Unit = {
        // 1. 创建连接
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("File Created RDD")

        val sparkContext = new SparkContext(conf)

        // 2. 读取文件数据
        // 读取文件的时候，可以使用正则通配符的方式，使用 * 替代多个字符；当然也可以读取 HDFS 文件目录
        val rdd: RDD[String] = sparkContext.textFile("./data/input/rddFile.txt")

        rdd.collect().foreach(println)

        // 3. 关闭连接
        sparkContext.stop()
    }

}
