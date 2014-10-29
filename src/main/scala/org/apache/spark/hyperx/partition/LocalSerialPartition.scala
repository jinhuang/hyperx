package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx._

import scala.collection.mutable
import scala.util.Random

class LocalSerialPartition extends GreedySerialPartition {
    override private[partition] def search() = {
        randomHyperedges()
        randomVertices()
        materialize()
        vertices.foreach(v => vertices.update(v._1, moveVertex(v._1, v._2)))
        var minimized = false
        while(!minimized) {
            val reduced = iterate()
            minimized = (reduced - 0.0) < EPSILON
        }
    }

    private def iterate(): Double = {
        val prevCost = cost()
        val avgD = avg(degrees)
        val avgR = avg(replicas)
        val candidateH = degrees.map(d => d > avgD)
        val fractionH = degrees.map(d => (d - avgD) * 10.0 / d)
            .map(f => if (f > 1.0) 1.0 else f)
        val candidateR = replicas.map(r => r < avgR)
        val fractionR = replicas.map(r => (avgR - r) * 10.0 / avgR)
            .map(f => if (f > 1.0) 1.0 else f)
        trackHyperedges.clear()
        trackVertices.clear()
        removeHyperedges(h =>
            candidateH(h._2) && Random.nextDouble() < fractionH(h._2))
        removeVertices(v =>
            candidateR(v._2) && Random.nextDouble() < fractionR(v._2))
        hyperedges.filter(_._2 == unassignedPid).foreach{h =>
            addHyperedge(h._1)
        }
        vertices.filter(_._2 == unassignedPid).foreach{v =>
            addVertex(v._1)
        }
        val afterCost = cost()
        if ((prevCost - afterCost) < 0.0) {
            trackHyperedges.foreach(h => assign(h._1, h._2))
            trackVertices.foreach(v => assign(v._1, v._2))
            0.0
        }
        else {
            logInfo("HYPERX: Local cost " + afterCost)
            prevCost - afterCost
        }
    }


    private[partition] def removeHyperedges (
        condition: ((String, PartitionId))=> Boolean) = {
        hyperedges.filter(condition).foreach{h =>
            trackHyperedges.update(h._1, h._2)
            hyperedges.update(h._1, unassignedPid)}
    }

    private[partition] def removeVertices (
        condition: ((VertexId, PartitionId)) => Boolean) = {
        vertices.filter(condition).foreach{v =>
            trackVertices.update(v._1, v._2)
            vertices.update(v._1, unassignedPid)}
    }

    private val EPSILON = 0.05

    private val trackHyperedges = new mutable.HashMap[String, PartitionId]()
    private val trackVertices = new mutable.HashMap[VertexId, PartitionId]()
}
