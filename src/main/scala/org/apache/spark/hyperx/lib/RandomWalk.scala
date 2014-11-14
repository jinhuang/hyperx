package org.apache.spark.hyperx.lib

import org.apache.spark.Logging
import org.apache.spark.hyperx._

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
    :Hypergraph[Double, _] = {
        val num = hypergraph.numVertices
        run(hypergraph, 10, hypergraph.pickRandomVertices(num.toInt))
    }

    def run[VD: ClassTag, ED: ClassTag]( hypergraph: Hypergraph[VD, ED],
        num: Int, maxIter: Int)
    :Hypergraph[Double, _] = {
        run(hypergraph, maxIter, hypergraph.pickRandomVertices(num))
    }

    def run[VD: ClassTag, ED: ClassTag](hypergraph: Hypergraph[VD, ED],
        numIter: Int, startSet: mutable.HashSet[VertexId], resetProb: Double = 0.15)
    : Hypergraph[Double, _] = {

        val walkHypergraph: Hypergraph[(Double, Int), Boolean] = hypergraph
            .outerJoinVertices(hypergraph.outIncidents){(vid, vdata, deg) =>
                deg.getOrElse[Int](0)}
            .mapTuples(h => (h.id, null.asInstanceOf[Boolean]))
            .mapVertices((id, attr) => (if (startSet.contains(id)) 1.0 else 0.0, attr))
            .cache()


        def vertexProg(id: VertexId, attr: (Double, Int), msgSum: Double): (Double, Int) = {
            if (startSet.contains(id)) {
                (resetProb * 1.0 + (1 - resetProb) * msgSum, attr._2)
            } else {
                ((1 - resetProb) * msgSum, attr._2)
            }
        }

        def hyperedgeProg(hyperedge: HyperedgeTuple[(Double, Int), Boolean])
//            srcAcc: Accumulator[Int], dstAcc: Accumulator[Int],
//            srcDAcc: Accumulator[Int], dstDAcc: Accumulator[Int])
        = {
            val dstSize = hyperedge.dstAttr.size
            val msgVal = hyperedge.srcAttr.zipWithIndex.map{attr => attr._1._2._1 / attr._1._2._2}.sum / dstSize
            if (msgVal > 0.0)
                hyperedge.dstAttr.map(attr => (attr._1, msgVal)).toIterator
            else
                Iterator.empty
        }

        def messageCombiner(a: Double, b: Double): Double = a + b

        val initialMessage = 0.0

        val ret = HyperPregel(walkHypergraph, initialMessage, numIter,
            activeDirection = HyperedgeDirection.Out)(
                    vertexProg, hyperedgeProg, messageCombiner)

        ret.mapVertices((id, attr) => attr._1)

    }

}
