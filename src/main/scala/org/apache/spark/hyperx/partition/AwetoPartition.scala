package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.rdd.RDD

import scala.util.Random

class AwetoPartition extends PartitionStrategy{
    override private[partition] def search(input: RDD[String]): Unit = {
        // randomly assign vertices
        vRDD = input.flatMap(h => HyperUtils.iteratorFromHString(h))
            .distinct(k).map(v => (v, Random.nextInt(k))).cache()

        // greedily assign hyperedges
        hRDD = input.mapPartitions{part =>
            val degrees = Array.fill(k)(0)
            part.map{h =>
                val pid = (0 until k).map(i =>
                    (i, degrees(i))).minBy(_._2)._1
                degrees(pid) += HyperUtils.countDegreeFromHString(h)
                (h, pid)
        }}.cache()

    }
}
