package org.apache.spark.hyperx.lib

import org.apache.spark.{Accumulator, Logging}
import org.apache.spark.hyperx.{HyperPregel, HyperedgeTuple, Hypergraph, VertexId}

import scala.reflect.ClassTag

/**
 * Compute the static label propagation on the hypergraph.
 *
 * The procedure is similar to the label propagation on a graph,
 * the difference is as follows:
 * 1. The messages are sent and received between every pair of vertices in a
 * hyperedge
 * 2. The label weight is a fraction depending on the destination vertex set
 * size, not 1
 *
 * Similar to the graph implementation, the procedure may not converge and
 * may terminates in
 * trivial states, e.g., every vertex is a community
 */
object LabelPropagation extends Logging {
    def run[VD: ClassTag, ED: ClassTag]
        (hypergraph: Hypergraph[VD, ED], numIter: Int)
    : Hypergraph[VertexId, ED] = {

        val lpHypergraph = hypergraph.mapVertices((id, _) => id)

        val sc = lpHypergraph.vertices.context

        def hyperedgeProgram(h: HyperedgeTuple[VertexId, ED],
            srcAcc: Accumulator[Int], dstAcc: Accumulator[Int],
            srcDAcc: Accumulator[Int], dstDAcc: Accumulator[Int]
                                ) = {
//            val msg = (h.srcAttr.map(_._2) ++ h.dstAttr.map(_._2)).map(v => (v, 1L)).toMap
//            (h.srcAttr.keySet.iterator ++ h.dstAttr.keySet.iterator).map(v => (v, msg))
            srcDAcc += h.srcAttr.size
            dstDAcc += h.dstAttr.size
            var start = System.currentTimeMillis()
            val srcMsg = h.srcAttr.map(_._2).groupBy(v => v).mapValues(iter => iter.size).maxBy(_._2)._1
            srcAcc += (System.currentTimeMillis() - start).toInt
            start = System.currentTimeMillis()
            val dstMsg = h.dstAttr.map(_._2).groupBy(v => v).mapValues(iter => iter.size).maxBy(_._2)._1
            dstAcc += (System.currentTimeMillis() - start).toInt
            h.srcAttr.map(v => (v._1, Map(dstMsg -> 1L))).iterator ++ h.dstAttr.map(v => (v._1, Map(srcMsg -> 1L))).iterator
        }

        def mergeMessage(count1: Map[VertexId, Long],
            count2: Map[VertexId, Long])
        : Map[VertexId, Long] = {
            (count1.keySet ++ count2.keySet).map { i =>
                val count1Val = count1.getOrElse(i, 0L)
                val count2Val = count2.getOrElse(i, 0L)
                i -> (count1Val + count2Val)
            }.toMap
        }

        def vertexProgram(vid: VertexId, attr: VertexId,
            message: Map[VertexId, Long]) = {
            if (message == null || message.isEmpty) attr else message.maxBy(_._2)._1
        }

        val initialMessage: Map[VertexId, Long] = null.asInstanceOf[Map[VertexId, Long]]

        HyperPregel.run[VertexId, ED, Map[VertexId, Long]](sc, lpHypergraph,
            initialMessage, maxIterations = numIter)(vprog = vertexProgram,
                    hprog = hyperedgeProgram, mergeMsg = mergeMessage)
    }
}
