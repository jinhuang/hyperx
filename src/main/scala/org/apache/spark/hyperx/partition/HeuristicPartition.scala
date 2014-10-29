package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap

import scala.collection.mutable

abstract class HeuristicPartition extends PartitionStrategy {
    private[partition] var k = 0
    var numPartitions = k

    private[partition] val hyperedges: HyperXOpenHashMap[String, PartitionId]
    = new HyperXOpenHashMap[String, PartitionId]()

    private[partition] val vertices: HyperXOpenHashMap[VertexId, PartitionId]
    = new HyperXOpenHashMap[VertexId, PartitionId]()

    /** materialized statistics and structures for computing the cost */
    private[partition] var degrees: Array[Int] = null
    private[partition] var replicas: Array[Int] = null
    private[partition] var locals:Array[mutable.HashSet[VertexId]] = null
    private[partition] var demands: Array[mutable.OpenHashMap[VertexId, Int]] = null

    private[partition] def materialize() = {
        locals = localVertices()
        demands = demandVertices()
        degrees = degreeCount()
        replicas = replicaCount()
    }

    private[partition] def localVertices()
    : Array[mutable.HashSet[VertexId]] = {
        val local = Array.fill(k)(new mutable.HashSet[VertexId])
        vertices.foreach(v => if (v._2 >= 0) local(v._2).add(v._1))
        local
    }

    private[partition] def demandVertices()
    : Array[mutable.OpenHashMap[VertexId, Int]] = {
        demands = Array.fill(k)(new mutable.OpenHashMap[VertexId, Int]())
        hyperedges.foreach(h =>
            if (h._2 >= 0)
                HyperUtils.iteratorFromHString(h._1).foreach(v =>
                    addVertexToDemand(h._2, v))
        )
        demands
    }

    private[partition] def degreeCount(): Array[Int] = {
        val degreeCount = Array.fill(k)(0)
        hyperedges.foreach{h =>
            val degree = HyperUtils.countDetailDegreeFromHString(h._1)
            val effective = HyperUtils.effectiveCount(degree._1, degree._2)
            if (h._2 >= 0) degreeCount(h._2) += effective
        }
        degreeCount
    }

    private[partition] def replicaCount(): Array[Int] = {
        (0 until k).map{i =>
            val demand = demands(i).iterator.map(v => v._1).toSet
            val local = locals(i)
            demand.size - HyperUtils.countIntersect(demand, local)
        }.toArray
    }

    private[partition] def cost(): Double = {
        2 * replicas.sum +
                costReplica * dvt(replicas) +
                costHyperedge * dvt(degrees) +
                costDemand * dvt(demands.map(_.size))
    }

    private[partition] def avg(array: Array[Int]): Double =
        array.sum * 1.0 / array.length

    private[hyperx] def dvt(array: Array[Int]): Double = {
        val average = avg(array)
        val sum = array.map(e => Math.pow(Math.abs(e - average), norm)).sum
        Math.pow(sum, 1.0 / norm)
    }

    private[partition] def hasDemand(p: PartitionId, v: VertexId): Boolean = {
        demands(p).contains(v) && demands(p)(v) > 0
    }

    private[partition] def addVertexToDemand(p: PartitionId, v: VertexId): Unit = {
        if (demands(p).contains(v)) {
            demands(p).update(v, demands(p)(v) + 1)
        }
        else {
            demands(p).update(v, 1)
        }
    }

    private[partition] def removeVertexFromDemand(p: PartitionId, v: VertexId): Unit = {
        if (demands(p).contains(v)) {
            if (demands(p)(v) > 0) demands(p).update(v, demands(p)(v) - 1)
            if (demands(p)(v) == 0) demands(p).remove(v)
        }
    }

    private[hyperx] def getHyperedges:Iterator[(String, PartitionId)] = {
        hyperedges.iterator
    }

    private[hyperx] def getHyperedges(part: PartitionId): Iterator[(String, PartitionId)] = {
        hyperedges.iterator.filter(_._2 == part)
    }

    private[hyperx] def getVertices: Iterator[(VertexId, PartitionId)] = {
        vertices.iterator
    }

    private[partition] def setHyperedges(key: String, value: PartitionId) = {
        hyperedges.update(key, value)
    }

    private[partition] def setVertices(key: VertexId, value: PartitionId) = {
        vertices.update(key, value)
    }

    private[partition] def setK(k_ :Int) = {
        k = k_
    }

    private[hyperx] def setObjectiveParams(h: Double, v: Double, d: Double, norm_ : Int) = {
        costHyperedge = h
        costReplica = v
        costDemand = d
        norm = norm_
    }

    private[hyperx] def getPartitioner: VertexPartitioner = {
        new VertexPartitioner(k, vertices)
    }

    private[partition] def printFinalMsg(start: Long) = {
        logInfo("HYPERX PARTITION: cost %s in %d ms hyperedges: %d vertices: %d"
            .format(cost(), System.currentTimeMillis() - start,
                hyperedges.size, vertices.size))
        logInfo("HYPERX PARTITION: numReplicas " + replicas.sum +
                " numDemands " + demands.map(_.size).sum +
//                " stdReplicas " + dvt(replicas) +
                " stdDegrees " + dvt(degrees) +
//                " replicas " + (0 until k).map{i => i + ": " + replicas(i)}.reduce(_ + " " + _ ) +
                " stdDemands " + dvt(demands.map(_.size)) +
//                " demands " + (0 until k).map{i => i + ": " + demands(i).size}.reduce(_ + " " + _ )
                " stdLocals " + dvt(locals.map(p => p.size))
        )
    }

    private[hyperx] def clear() = {
        // clear everything except the vertices (which are used as the
        // partitioner later)
        hyperedges.clear()
        degrees = null
        replicas = null
        locals = null
        demands = null
        // try to gc
        Runtime.getRuntime.gc()
    }

    private[partition] val unassignedPid = -1
}
