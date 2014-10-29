package org.apache.spark.hyperx.lib

import org.apache.spark.Logging
import org.apache.spark.hyperx._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import scala.reflect.ClassTag

/**
 * Compute the transition probabilities for Eigenvalue analytics and learning
 * tasks.
 *
 * The transition is carried out by first picking an outer hyperedge and then
 * picking a vertex in the destination vertex set of that hyperedge, hence the
 * probabilities are computed as 1 / (outIncidents * hyperedge size)
 *
 * The returning RDD contains a collection of Maps, each Map corresponds to
 * one source vertex, the Map key corresponds to the destination vertex of the
 * transition and the Map value corresponds to the probability
 */
object TransitionProbabilities extends Logging {

    def run[VD: ClassTag, ED: ClassTag](hypergraph: Hypergraph[VD,ED])
    : VertexRDD[Map[BitSet, Double]] = {

        println("HYPERX DEBUGGING: to compute incidents ")

//        val neighborsRDD = hypergraph.collectNeighborIds(HyperedgeDirection.Out)

        val incidentHypergraph = hypergraph.outerJoinVertices(hypergraph.outIncidents){
            (vid, vdata, deg) => deg.getOrElse[Int](0)}.cache()
        println("HYPERX DEBUGGING: finished incident, to compute mapReduceTuple")

        val iteratorRDD = incidentHypergraph.mapReduceTuples[Iterator[(BitSet, Double)]](mapHyperedges[ED], _ ++ _)

        iteratorRDD.mapValues(it =>
             it.toMap
        )
    }

    def mapHyperedges[ED: ClassTag](tuple: HyperedgeTuple[Int, ED])
    : Iterator[(VertexId, Iterator[(BitSet, Double)])] = {
        val hyperedgeSize = tuple.dstAttr.size
        val dstSet = new OpenHashSet[VertexId]()
        tuple.dstAttr.foreach(attr => dstSet.add(attr._1))
        val dstBitset = dstSet.getBitSet
        tuple.srcAttr.map{attr =>
            val incidentSize = attr._2
            (attr._1, Iterator((dstBitset, 1.0 / hyperedgeSize / incidentSize)))
        }.iterator
    }
}
