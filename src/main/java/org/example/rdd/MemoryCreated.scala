package org.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 本示例是为了演示 RDD 的创建几种方式中，使用内存创建的方案
 */
object MemoryCreated {
    def main(args: Array[String]): Unit = {
        // 1. 准备环境
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("RDD CreatedExample")

        val sparkContext = new SparkContext(conf)

        // 2. 创建 RDD
        val seq = Seq[Int](1, 2, 3, 4)
        val rdd: RDD[Int] = sparkContext.parallelize(seq, 2)

        rdd.collect().foreach(println)

        // 3. 关闭环境
        sparkContext.stop()

    }

}
