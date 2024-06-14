package com.example.exercise

import org.apache.spark.{SparkConf, SparkContext}

object RddExercise {
    def main(args: Array[String]): Unit = {
        // 1. 连接服务
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("RDD Exercise")

        val sparkContext = new SparkContext(conf)

        /**
        * 处理数据： 统计出每一个省份每个广告被点击数量排行的Top3
        * 原始数据的字段信息为： 时间戳、省份、城市、用户和广告
        */
        val rdd = sparkContext.textFile("./data/input/exercise/agent.log")
            .map((line) => {
                val words = line.split(" ")
                ((words(1), words(4)), 1)
            })
            .reduceByKey(_ + _)
            .map({
                case ((province, ad), total) => (province, (ad, total))
            })
            .groupByKey()
            .mapValues((iter)=>{
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            })

        rdd.collect().foreach(println)


        // 3. 关闭服务
        sparkContext.stop()
    }

}
