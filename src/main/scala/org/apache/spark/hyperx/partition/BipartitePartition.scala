package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.rdd.RDD

import scala.util.Random

class BipartitePartition extends PartitionStrategy {
    override private[partition] def search(input: RDD[String]): Unit = {
        // arbitrarily assign hyperedges
        hRDD = input.map(h => (h, Random.nextInt(k))).cache()

        // randomly assign vertices
        vRDD = hRDD.flatMap(h => HyperUtils.iteratorFromHString(h._1))
            .distinct(k).map(v => (v, Random.nextInt(k))).cache()
    }
}
