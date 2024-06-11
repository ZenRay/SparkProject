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
        println("使用 textFile 返回的内容: ")
        val rdd: RDD[String] = sparkContext.textFile("./data/input/rdd/rddFile.txt")
        rdd.collect().foreach(println)

        // 另一中读取方法是使用 wholeTextFiles，它会返回文件路径和数据内容(数据内容是全部的内容)构成的 "元组"
        // 需要注意传入的参数是一个 Path
        println("使用 wholeTextFiles 返回的内容: ")
        val value = sparkContext.wholeTextFiles("./data/input/rdd")
        value.collect().foreach(println)
        

        // 3. 关闭连接
        sparkContext.stop()
    }

}
