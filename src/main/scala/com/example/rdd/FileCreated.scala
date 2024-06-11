package com.example.rdd

import org.apache.spark.{SparkConf, SparkContext}

object FileCreated {
    def main(args: Array[String]): Unit = {
        // 1. 创建连接
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setMaster("File Created RDD")

        val sparkContext = new SparkContext(conf)

        // 2. 读取文件数据
        sparkContext.textFile()
    }

}
