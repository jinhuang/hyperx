package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.{PartitionId, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.Random


class PlainPartition extends HeuristicPartition {
    override def partition(numParts: PartitionId, input: RDD[String]): (RDD[
            (PartitionId, VertexId)], RDD[(PartitionId, String)]) = {
        k = numParts
        val hyperedgeRDD = input.coalesce(k, shuffle = true)
            .mapPartitionsWithIndex({(i, p) =>
                p.map(s => (i, s))
            }, preservesPartitioning = true)
        val vertexRDD = hyperedgeRDD.flatMap(h =>
            HyperUtils.iteratorFromHString(h._2)).distinct(k)
            .map(v => (Random.nextInt(k), v))
        vertexRDD.collect().foreach(v => vertices.update(v._2, v._1))
        (vertexRDD, hyperedgeRDD)
    }
}
