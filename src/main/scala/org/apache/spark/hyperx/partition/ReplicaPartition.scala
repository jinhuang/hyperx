package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.VertexId
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.util.collection.HyperXOpenHashSet
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created by soone on 11/25/14.
 */
class ReplicaPartition extends PartitionStrategy{
    override private[partition] def search(input: RDD[String]): Unit = {

        hRDD = input.coalesce(1).mapPartitions{p =>
            val sets = Array.fill(k)(new HyperXOpenHashSet[VertexId]())
            val degrees = Array.fill(k)(0)
            p.map { h =>
                val hSet = HyperUtils.setFromHString(h)
                val pid = (0 until k).map(i => (i, sets(i).iterator.count(hSet.contains) * costReplica - costDegree * degrees(i))).maxBy(_._2)._1
                hSet.iterator.foreach(sets(pid).add)
                degrees(pid) += hSet.size
                (h, pid)
            }
        }.cache()

        vRDD = hRDD.flatMap(h => HyperUtils.iteratorFromHString(h._1))
            .distinct(k).map{v =>
            val pid = Random.nextInt(k)
            (v, pid)
        }.cache()
    }
}
