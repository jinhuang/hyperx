package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap

class GreedyPartition extends SerialPartition{
    override private[partition] def search(): Unit = {
        greedyHyperedges()
        greedyVertices()
        materialize()
    }

    // greedy balance the degrees and demands
    private[partition] def greedyHyperedges(): Unit = {
        degrees = Array.fill(k)(0)
        demands = Array.fill(k)(new HyperXOpenHashMap[VertexId, Int]())
        hyperedges.foreach { h =>

            val choice = (0 until k).map(i =>
                (i, degrees(i) * costHyperedge +
                    demands(i).size * costDemand)).minBy(_._2)._1
            val degree = HyperUtils.countDetailDegreeFromHString(h._1)
            val equvCount = HyperUtils.effectiveCount(degree._1, degree._2)
            degrees(choice) += equvCount.toInt
            HyperUtils.iteratorFromHString(h._1).foreach{v =>
                if (!demands(choice).hasKey(v))
                    demands(choice).update(v, 1)
            }
            hyperedges.update(h._1, choice)
        }
    }

    // greedy balance the number of vertices
    private[partition] def greedyVertices(): Unit ={
        val localCount = Array.fill(k)(0)
        hyperedges.foreach{h =>
            HyperUtils.iteratorFromHString(h._1).foreach{v =>
                if (vertices(v) == unassignedPid) {
                    vertices(v) = h._2
                    localCount(h._2) += 1
                } else if ((localCount(vertices(v)) - localCount(h._2)) > 0) {
                    localCount(vertices(v)) -= 1
                    vertices(v) = h._2
                    localCount(h._2) += 1
                }
            }
        }
    }
}
