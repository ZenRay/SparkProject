package com.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Parallelism {
    def main(args: Array[String]): Unit = {
        // 1. 创建连接
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("File Created RDD")

        val sparkContext = new SparkContext(conf)

        // 2. 创建 RDD
        // 可以使用 makdRDD，和 parallize 的第二参数一样都是并行度
        /**
         * 没有配置 numSlices 情况下，可以读取 Spark 配置的 spark.default.parallelism
         * 还没有的情况时，会取环境的总核数 totalCores。需要注意切分的方式
         */
        val seq = Seq[Int](1, 2, 3, 4)
        // val rdd: RDD[Int] = sparkContext.parallelize(seq, 2)
        val rdd: RDD[Int] = sparkContext.makeRDD(seq, numSlices = 2)

        // 将 RDD数据保存，保存的文件数量和分区的数量一致
        rdd.saveAsTextFile("./data/output/rdd")

        // 3. 关闭环境
        sparkContext.stop()

    }

}
