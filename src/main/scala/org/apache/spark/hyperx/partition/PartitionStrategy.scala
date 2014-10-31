package org.apache.spark.hyperx.partition

import org.apache.spark.Logging
import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
/**
 * Represents the way hyperedges and vertices are assigned to partitions.
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */
trait PartitionStrategy extends Serializable with Logging {

    private[partition] def search(input: RDD[String]): Unit

    def partition(numParts: PartitionId, input: RDD[String])
    : (RDD[(PartitionId, VertexId)], RDD[(PartitionId, String)]) = {
        val start = System.currentTimeMillis()
        k = numParts
        search(input)
        logInfo("HYPERX PARTITION: partition in %d ms".format(System.currentTimeMillis() - start))
        printStatistics()
        (vRDD.map(v => (v._2, v._1)), hRDD.map(h => (h._2, h._1)))
    }

    def getPartitioner: VertexPartitioner = {
        VertexPartitioner(k, vRDD.collect().iterator)
    }

    private[partition] var vRDD: RDD[(VertexId, PartitionId)] = _
    private[partition] var hRDD: RDD[(String, PartitionId)] = _

    def setObjectiveParams(h: Double, v: Double, d: Double, norm_ : Int) = {
        costHyperedge = h
        costReplica = v
        costDemand = d
        norm = norm_
    }

    private def printStatistics(): Unit = {
        // demands
        val demands = hRDD.map(h => Tuple2(h._2, HyperUtils.iteratorFromHString(h._1).toSet)).reduceByKey(_.union(_)).map(_._2.size).collect()
        logInfo("HYPERX PARTITION: demands avg %d min %d max %d std %d".format(HyperUtils.avg(demands).toInt, demands.min, demands.max, HyperUtils.dvt(demands).toInt))

        // degrees
        val degrees = hRDD.map(h => Tuple2(h._2, HyperUtils.countDegreeFromHString(h._1))).reduceByKey(_ + _).map(_._2).collect()
        logInfo("HYPERX PARTITION: degrees avg %d min %d max %d std %d".format(HyperUtils.avg(degrees).toInt, degrees.min, degrees.max, HyperUtils.dvt(degrees).toInt))

        // locals
        val locals = vRDD.map(v => Tuple2(v._2, 1)).reduceByKey(_ + _).map(_._2).collect()
        logInfo("HYPERX PARTITION: locals avg %d min %d max %d std %d".format(HyperUtils.avg(locals).toInt, locals.min, locals.max, HyperUtils.dvt(locals).toInt))
    }

    private[partition] var k: Int = 0
}