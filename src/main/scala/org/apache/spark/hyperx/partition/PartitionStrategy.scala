package org.apache.spark.hyperx.partition

import org.apache.spark.{HashPartitioner, Logging}
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
        logInfo("HYPERX PARTITION: partition in %d ms".format(
            System.currentTimeMillis() - start))
        printStatistics()
        (vRDD.map(v => (v._2, v._1)), hRDD.map(h => (h._2, h._1)))
    }

    def getPartitioner: VertexPartitioner = {
        VertexPartitioner(k, vRDD.collect().iterator)
    }

    private[partition] var vRDD: RDD[(VertexId, PartitionId)] = _
    private[partition] var hRDD: RDD[(String, PartitionId)] = _

    def setPartitionParams(degreeCost: Double, replicaCost: Double, 
        demandCost: Double, normSpace: Int, effSrc: Double, effDst: Double) = {
        costDegree = degreeCost
        costReplica = replicaCost
        costDemand = demandCost
        norm = normSpace
        effectiveSrc = effSrc
        effectiveDst = effDst
    }

    private def printStatistics(): Unit = {

        val numH = hRDD.count()
        val numV = vRDD.count()
        logInfo("HYPERX PARTITION: hyperedges %d vertices %d".format(numH, numV))

        // demands
        val demands = hRDD.map(h =>
            Tuple2(h._2, HyperUtils.iteratorFromHString(h._1).toSet))
            .reduceByKey(_.union(_)).partitionBy(new HashPartitioner(k)).cache()
        val locals = vRDD.map(v => Tuple2(v._2, Set(v._1))).reduceByKey(_ ++ _).partitionBy(new HashPartitioner(k)).cache()

        logInfo("HYPERX DEBUGGING: demands " + demands.map(each => each._1 + " : " + each._2.size).reduce(_ + " ; " + _ ))
        logInfo("HYPERX DEBUGGING: locals " + locals.map(each => each._1 + " : " + each._2.size).reduce(_ + " ; " + _))

        val replicas = demands.zipPartitions(locals){(d, l) =>
            val dSet = d.filter(_._2.size > 0).map(_._2).reduce(_ ++ _)
            val lSet = l.filter(_._2.size > 0).map(_._2).reduce(_ ++ _)
            Iterator(dSet.size - dSet.intersect(lSet).size)
        }.cache()

        logArray("replicas", replicas.collect())
        logArray("demands", demands.map(_._2.size).collect())
        logArray("locals", locals.map(_._2.size).collect())
        logInfo("HYPERX PARTITION: replicaFactor: %f".format(replicas.sum / vRDD.count()))
        // degrees
        val degrees = hRDD.map{h =>
            val pair = HyperUtils.countDetailDegreeFromHString(h._1)
            Tuple2(h._2, pair)}
            .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(_._2)
            .collect()
        val srcDegrees = degrees.map(_._1)
        val dstDegrees = degrees.map(_._2)
        logArray("source degrees", srcDegrees)
        logArray("destination degrees", dstDegrees)

        demands.unpersist(blocking = false)
        locals.unpersist(blocking = false)
        replicas.unpersist(blocking = false)

//        val localsCount = vRDD.map(v => Tuple2(v._2, 1))
//          .reduceByKey(_ + _).map(_._2).collect()
    }

    private def logArray(name: String, ary: Array[Int]): Unit = {
        logInfo("HYPERX PARTITION: %s avg %d min %d max %d std %d std percent %f"
            .format(name, HyperUtils.avg(ary).toInt, ary.min, ary.max,
            HyperUtils.dvt(ary).toInt, HyperUtils.dvt(ary) / HyperUtils.avg(ary)
          ))
    }

    private[partition] var k: Int = 0
}

object PartitionStrategy extends Logging {
    def printStatistics(hRDD: RDD[(String, PartitionId)], vRDD: RDD[(VertexId, PartitionId)], k: Int): Unit = {

        val numH = hRDD.count()
        val numV = vRDD.count()
        logInfo("HYPERX PARTITION: hyperedges %d vertices %d".format(numH, numV))

        // demands
        val demands = hRDD.map(h =>
            Tuple2(h._2, HyperUtils.iteratorFromHString(h._1).toSet))
            .reduceByKey(_.union(_)).partitionBy(new HashPartitioner(k)).cache()
        val locals = vRDD.map(v => Tuple2(v._2, Set(v._1))).reduceByKey(_ ++ _).partitionBy(new HashPartitioner(k)).cache()

        val replicas = demands.zipPartitions(locals){(d, l) =>
            val dSet = d.filter(_._2.size > 0).map(_._2).reduce(_ ++ _)
            val lSet = l.filter(_._2.size > 0).map(_._2).reduce(_ ++ _)
            Iterator(dSet.size - dSet.intersect(lSet).size)
        }.cache()

        logArray("replicas", replicas.collect())
        logArray("demands", demands.map(_._2.size).collect())
        logArray("locals", locals.map(_._2.size).collect())
        logInfo("HYPERX PARTITION: replicaFactor: %f".format(replicas.sum / vRDD.count()))
        // degrees
        val degrees = hRDD.map{h =>
            val pair = HyperUtils.countDetailDegreeFromHString(h._1)
            Tuple2(h._2, pair)}
            .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(_._2)
            .collect()
        val srcDegrees = degrees.map(_._1)
        val dstDegrees = degrees.map(_._2)
        logArray("source degrees", srcDegrees)
        logArray("destination degrees", dstDegrees)

        demands.unpersist(blocking = false)
        locals.unpersist(blocking = false)
        replicas.unpersist(blocking = false)

        //        val localsCount = vRDD.map(v => Tuple2(v._2, 1))
        //          .reduceByKey(_ + _).map(_._2).collect()
    }

    private def logArray(name: String, ary: Array[Int]): Unit = {
        logInfo("HYPERX PARTITION: %s avg %d min %d max %d std %d std percent %f"
            .format(name, HyperUtils.avg(ary).toInt, ary.min, ary.max,
                HyperUtils.dvt(ary).toInt, HyperUtils.dvt(ary) / HyperUtils.avg(ary)
            ))
    }
}