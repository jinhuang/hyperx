package org.apache.spark.hyperx.partition.obsolete

import org.apache.spark.hyperx._
import org.apache.spark.hyperx.partition._
import org.apache.spark.hyperx.util.collection.HyperXOpenHashSet
import org.apache.spark.rdd.RDD

import scala.util.Random


abstract class ParallelPartition extends HeuristicPartition {
    override def partition(numPart: Int, input: RDD[String])
    :(RDD[(PartitionId, VertexId)], RDD[(PartitionId, String)])= {
        val start = System.currentTimeMillis()
        val sc = input.context
        val result = input.map((_, Random.nextInt(numPart)))
                .coalesce(numPart, shuffle = true)
                .mapPartitions{part =>
            strategy.partition(numPart, part)
            Iterator((strategy.getHyperedges, strategy.getVertices))
        }.collect()
        val verticesToFurtherConsider = new HyperXOpenHashSet[VertexId]()
        result.foreach{p =>
            p._1.foreach(h => hyperedges.update(h._1, h._2))
            p._2.foreach{v =>
                if (vertices.hasKey(v._1)) verticesToFurtherConsider.add(v._1)
                vertices.update(v._1, v._2)
            }
        }
        k = numPart
        numPartitions = k
        materialize()
        // resolve the vertex assignment conflict
        verticesToFurtherConsider.iterator.foreach{v =>
            moveVertex(v, vertices(v))
        }
        printFinalMsg(start)
        (sc.parallelize(vertices.map(v => (v._2, v._1)).toSeq),
            sc.parallelize(hyperedges.map(h => (h._2, h._1)).toSeq))
    }

    private def moveVertex(v: (VertexId, PartitionId))
    : Unit = {
        val relevant = (0 until k).map(i => hasDemand(i, v._1))
        val costPairs = (0 until k).map(i => (i, costMoveVertex(v._1, v._2, i, relevant(v._2), relevant(i))))
        val minCost = costPairs.minBy(_._2)._2
        val newPid = costPairs.filter(p => (p._2 - minCost) <= 0.00001).map(p => (p._1, locals(p._1).size)).minBy(_._2)._1
        vertices.update(v._1, newPid)
        moveMaterialized(v._1, v._2, newPid)
    }

    private def costMoveVertex(v: VertexId, from: PartitionId,
                               to: PartitionId, relevantFrom: Boolean, relevantTo: Boolean): Double = {
        if (from == to) {
            2 * replicas.sum + costReplica * dvt(replicas)
        }
        else {
            val assigned = replicas.map(r => r)
            if (relevantFrom) {
                assigned(from) += 1
            }
            if (relevantTo) {
                assigned(to) -= 1
            }
            2 * assigned.sum + costReplica * dvt(assigned)
        }
    }

    private def moveMaterialized(v: VertexId, from: PartitionId,
                                 to: PartitionId): Unit = {

        if (from != to) {

            val reducedReplicas = if (hasDemand(to, v)) 1 else 0
            val extraReplicas = if (hasDemand(from, v)) 1 else 0
            replicas(from) += extraReplicas
            replicas(to) -= reducedReplicas

            locals(from).remove(v)
            locals(to).add(v)
        }
        // demands remain unchanged
    }

    private[partition] val strategy: GreedySerialPartition
}
