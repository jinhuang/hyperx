package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.util.Random

class AwetoPartition extends PartitionStrategy{
    override private[partition] def search(input: RDD[String]): Unit = {
        // randomly assign vertices
        vRDD = input.flatMap(h => HyperUtils.iteratorFromHString(h))
            .distinct(k).map(v => (v, Random.nextInt(k))).cache()

        val locals = vRDD.map(v => (v._2, Set(v._1)))
            .reduceByKey(_ ++ _).collectAsMap()


        // greedily assign hyperedges
        hRDD = input.mapPartitions{part =>
            val degrees = Array.fill(k)(0)
            part.map{h =>
                val hSet = HyperUtils.iteratorFromHString(h).toSet
                val pid = (0 until k).map(i =>
                    (i, Math.abs(hSet.size - hSet.intersect(locals(i)).size) -
                        Math.sqrt(degrees(i)))).maxBy(_._2)._1
                degrees(pid) += HyperUtils.countDegreeFromHString(h)
                (h, pid)
        }}.cache()

    }
}
