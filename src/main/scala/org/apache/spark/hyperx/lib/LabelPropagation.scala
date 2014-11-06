package org.apache.spark.hyperx.lib

import org.apache.spark.Logging
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

        def hyperedgeProgram(h: HyperedgeTuple[VertexId, ED]) = {
//            h.dstAttr.map(attr =>
//                (attr._1, h.srcAttr.map(src =>
//                    (src._2,1.0 / h.dstAttr.size)).toMap)).iterator ++
//            h.srcAttr.map(attr =>
//                (attr._1, h.dstAttr.map(dst =>
//                    (dst._2, 1.0 / h.srcAttr.size)).toMap)).iterator
            // exchange labels between source and destinations
            val srcSize = h.srcAttr.size
            val dstSize = h.dstAttr.size
            val srcMap = h.srcAttr.map(v => (v._2, 1.0 / dstSize)).toMap
            val dstMap = h.dstAttr.map(v => (v._2, 1.0 / srcSize)).toMap
            h.dstAttr.keySet.iterator.map(v => (v, srcMap)) ++
            h.srcAttr.keySet.iterator.map(v => (v, dstMap))
        }

        def mergeMessage(count1: Map[VertexId, Double],
            count2: Map[VertexId, Double])
        : Map[VertexId, Double] = {
            (count1.keySet ++ count2.keySet).map { i =>
                val count1Val = count1.getOrElse(i, 0D)
                val count2Val = count2.getOrElse(i, 0D)
                i -> (count1Val + count2Val)
            }.toMap
        }

        def vertexProgram(vid: VertexId, attr: VertexId,
            message: Map[VertexId, Double]) = {
            if (message.isEmpty) attr else message.maxBy(_._2)._1
        }

        val initialMessage = Map[VertexId, Double]()

        HyperPregel[VertexId, ED, Map[VertexId, Double]](lpHypergraph,
            initialMessage, maxIterations = numIter)(vprog = vertexProgram,
                    hprog = hyperedgeProgram, mergeMsg = mergeMessage)
    }
}
