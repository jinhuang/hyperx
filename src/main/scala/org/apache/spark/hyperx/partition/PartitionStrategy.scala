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

        val numH = hRDD.count()
        val numV = vRDD.count()
        logInfo("HYPERX PARTITION: hyperedges %d vertices %d".format(numH, numV))

        // demands
        val demands = hRDD.map(h => Tuple2(h._2, HyperUtils.iteratorFromHString(h._1).toSet)).reduceByKey(_.union(_)).map(_._2.size).collect()
        logArray("demands", demands)
//        logInfo("HYPERX PARTITION: demands avg %d min %d max %d std %d std percent %f"
//                .format(HyperUtils.avg(demands).toInt, demands.min, demands.max, HyperUtils.dvt(demands).toInt,
//                HyperUtils.dvt(demands) / HyperUtils.avg(demands)))

        // degrees
        val degrees = hRDD.map{h =>
            val pair = HyperUtils.countDetailDegreeFromHString(h._1)
            Tuple2(h._2, pair)}.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(_._2).collect()
        val srcDegrees = degrees.map(_._1)
        val dstDegrees = degrees.map(_._2)
        logArray("source degrees", srcDegrees)
        logArray("destination degrees", dstDegrees)

//        logInfo("HYPERX PARTITION: degrees avg %d min %d max %d std %d std percent %f"
//                .format(HyperUtils.avg(degrees).toInt, degrees.min, degrees.max, HyperUtils.dvt(degrees).toInt,
//                HyperUtils.dvt(degrees) / HyperUtils.avg(degrees)))

        // locals
        val locals = vRDD.map(v => Tuple2(v._2, 1)).reduceByKey(_ + _).map(_._2).collect()
        logArray("locals", locals)
//        logInfo("HYPERX PARTITION: locals avg %d min %d max %d std %d std percent %f"
//                .format(HyperUtils.avg(locals).toInt, locals.min, locals.max, HyperUtils.dvt(locals).toInt,
//                HyperUtils.dvt(locals) / HyperUtils.avg(locals)))
    }

    private def logArray(name: String, ary: Array[Int]): Unit = {
        logInfo("HYPERX PARTITION: %s avg %d min %d max %d std %d std percent %f"
            .format(name, HyperUtils.avg(ary).toInt, ary.min, ary.max, HyperUtils.dvt(ary).toInt,
                HyperUtils.dvt(ary) / HyperUtils.avg(ary)))
    }

    private[partition] var k: Int = 0
}