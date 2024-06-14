package com.example.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL {
    def main(args: Array[String]): Unit = {
        // 1. 配置信息
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("Spark SQL")

        // 2. 创建 SparkSession 以运行 SparkSQL
        val sparkSession = SparkSession.builder()
            .config(conf)
            .getOrCreate()


        // 4. 关闭环境
        sparkSession.close()

    }

}
