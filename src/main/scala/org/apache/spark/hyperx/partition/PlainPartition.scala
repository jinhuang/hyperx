package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.rdd.RDD

import scala.util.Random

class PlainPartition extends PartitionStrategy {
    override def search(input: RDD[String]): Unit = {
        hRDD = input.coalesce(k, shuffle = true)
            .mapPartitionsWithIndex({(i, p) =>
                p.map(s => (s, i))
            }, preservesPartitioning = true)
        vRDD = hRDD.flatMap(h =>
            HyperUtils.iteratorFromHString(h._1)).distinct(k)
            .map(v => (v, Random.nextInt(k)))
    }
}
