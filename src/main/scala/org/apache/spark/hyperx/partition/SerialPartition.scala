package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.{PartitionId, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.Random

abstract class SerialPartition extends HeuristicPartition {

    private[partition] def search()

    override def partition(numPart: Int, input: RDD[String])
    :(RDD[(PartitionId, VertexId)], RDD[(PartitionId, String)])= {
        k = numPart
        numPartitions = k
        val sc = input.context
        var start = System.currentTimeMillis()
        val localInput = input.collect()
        logInfo("HYPERX DEBUGGING: P0 partition.collect in %d ms".format(System.currentTimeMillis() - start))
        start = System.currentTimeMillis()
        localInput.foreach(h => hyperedges.update(h, unassignedPid))
        val maxVertexId = input.map{s =>
            HyperUtils.iteratorFromHString(s).max
        }.max().toInt
        (0 to maxVertexId).foreach(v => vertices.update(v, unassignedPid))
        logInfo("HYPERX DEBUGGING: P1 unassign in %d ms".format(System.currentTimeMillis() - start))
        start = System.currentTimeMillis()
        search()
        logInfo("HYPERX DEBUGGING: P2 search in %d ms".format(System.currentTimeMillis() - start))
        printFinalMsg(start)
        (sc.parallelize(vertices.map(v => (v._2, v._1)).toSeq), sc.parallelize(hyperedges.map(h => (h._2, h._1)).toSeq))
    }

    def partition(numPart: Int, input: Iterator[(String, PartitionId)])
    : Unit = {
        k = numPart
        numPartitions = k
        input.foreach{h =>
            hyperedges.update(h._1, h._2)
        }
        val maxVertexId = hyperedges.map{s =>
            HyperUtils.iteratorFromHString(s._1).max
        }.max.toInt
        (0 to maxVertexId).foreach(v => vertices.update(v, unassignedPid))
        val start = System.currentTimeMillis()
        search()
        printFinalMsg(start)
    }

    private[partition] def randomHyperedges() = {
        hyperedges.keySet.iterator.foreach{key =>
            hyperedges.update(key, Random.nextInt(k))
        }
    }

    private[partition] def randomVertices() = {
        vertices.keySet.iterator.foreach{v => vertices.update(v, Random.nextInt(k))}
    }

}
