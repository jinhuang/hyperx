package org.apache.spark.hyperx.lib

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag


object RandomWalkStarGraph extends Logging {

    def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] =
    {
        // Initialize the pagerankGraph with each edge attribute
        // having weight 1/outDegree and each vertex with attribute 1.0.
        val pagerankGraph: Graph[(Double, Double), Double] = graph
            // Associate the degree with each vertex
            .outerJoinVertices(graph.outDegrees) {
            (vid, vdata, deg) => deg.getOrElse(0)
        }
            // Set the weight on the edges based on the degree
            .mapTriplets( e => 1.0 / e.srcAttr )
            // Set the vertex attributes to (initalPR, delta = 0)
            .mapVertices( (id, attr) => (0.0, 0.0) )
            .cache()

        // Define the three functions needed to implement PageRank in the GraphX
        // version of Pregel
        def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
            val (oldPR, lastDelta) = attr
            val newPR = if (id < 2322299) oldPR + (1.0 - resetProb) * msgSum else msgSum
            (newPR, newPR - oldPR)
        }

        def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
            Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
        }

        def messageCombiner(a: Double, b: Double): Double = a + b

        // The initial message received by all vertices in PageRank
        val initialMessage = resetProb / (1.0 - resetProb)

        // Execute a dynamic version of Pregel.
        org.apache.spark.hyperx.exp.Pregel(pagerankGraph, initialMessage, numIter * 2, activeDirection = EdgeDirection.Out)(
            vertexProgram, sendMessage, messageCombiner)
            .mapVertices((vid, attr) => attr._1)
    } // end of deltaPageRank

}
