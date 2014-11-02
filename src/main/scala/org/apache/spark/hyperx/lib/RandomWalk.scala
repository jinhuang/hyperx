package org.apache.spark.hyperx.lib

import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.{Accumulator, Logging}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Run random walk on the hypergraph.
 *
 * The implementation is similar to the page rank implementation for the
 * graph counterpart, the difference is as follows:
 * 1. The transition probabilities between vertices are computed based on the
 * number of out incident hyperedges of the source vertex and  the destination
 * vertex set size (the size of the hyperedge)
 * 2. The hyperedge program will send multiple messages to the destination
 * vertices
 *
 * The default direction is set to [[HyperedgeDirection.Out]]
 */
object RandomWalk extends Logging {

    def run[VD: ClassTag, ED: ClassTag]( hypergraph: Hypergraph[VD, ED])
    :Hypergraph[Double, HyperAttr[Double]] = {
        val num = hypergraph.numVertices
        run(hypergraph, 10, hypergraph.pickRandomVertices(num.toInt))
    }

    def run[VD: ClassTag, ED: ClassTag]( hypergraph: Hypergraph[VD, ED],
        num: Int, maxIter: Int)
    :Hypergraph[Double, HyperAttr[Double]] = {
        run(hypergraph, maxIter, hypergraph.pickRandomVertices(num))
    }

    def run[VD: ClassTag, ED: ClassTag](hypergraph: Hypergraph[VD, ED],
        numIter: Int, startSet: mutable.HashSet[VertexId], resetProb: Double = 0.15)
    : Hypergraph[Double, HyperAttr[Double]] = {

        val walkHypergraph: Hypergraph[Double, HyperAttr[Double]] = hypergraph
            .outerJoinVertices(hypergraph.outIncidents){(vid, vdata, deg) =>
                deg.getOrElse[Int](0)}
            .mapTuples(e => HyperUtils.divide(1.0, e.srcAttr))
            .mapVertices((id, attr) => if (startSet.contains(id)) 1.0 else 0.0)
            .cache()

        logInfo("Walking...")

        def vertexProg(id: VertexId, attr: Double, msgSum: Double): Double =
            resetProb + (1.0 - resetProb) * msgSum

        def hyperedgeProg(hyperedge: HyperedgeTuple[Double,HyperAttr[Double]],
            srcAcc: Accumulator[Int], dstAcc: Accumulator[Int],
            srcDAcc: Accumulator[Int], dstDAcc: Accumulator[Int])
        = {
            var start = System.currentTimeMillis()
            val dstSize = hyperedge.dstAttr.size
            val srcArray = Array.fill(hyperedge.srcAttr.size)(0)
            val msgVal = hyperedge.srcAttr.zipWithIndex.map{attr =>
                val start = System.currentTimeMillis()
                val ret = attr._1._2 * hyperedge.attr(attr._1._1)
                srcArray(attr._2) = (System.currentTimeMillis() - start).toInt
                ret
            }.sum * 1.0 / dstSize
            srcAcc += (System.currentTimeMillis() - start).toInt
            start = System.currentTimeMillis()
            val ret = hyperedge.dstAttr.map(attr => (attr._1, msgVal)).toIterator
            dstAcc += (System.currentTimeMillis() - start).toInt
            srcDAcc += hyperedge.srcAttr.size
            dstDAcc += hyperedge.dstAttr.size
            ret
        }

        def messageCombiner(a: Double, b: Double): Double = a + b

        val initialMessage = 0.0

        HyperPregel.run(walkHypergraph.hyperedges.context, walkHypergraph, initialMessage, numIter,
            activeDirection = HyperedgeDirection.Out)(
                    vertexProg, hyperedgeProg, messageCombiner)



    }

}
