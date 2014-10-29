package org.apache.spark.hyperx.lib

import org.apache.spark.Logging
import org.apache.spark.hyperx.{HyperedgeDirection, Hypergraph, VertexRDD}

import scala.reflect.ClassTag

/**
 * Compute the clustering coefficients for every vertex in the hypergraph
 *
 * @todo add independent and dependent clustering coefficients, which requires
 *       a pair combinatorial computations for every vertex; the problem is how
 *       to efficiently parallelize them on the RDD semantics
 */
object ClusterCoefficients extends Logging {

    def run[VD: ClassTag, ED: ClassTag](hypergraph: Hypergraph[VD,
            ED]): VertexRDD[Double] = {
        val neighbors = hypergraph.degrees
        val hyperedges = hypergraph.collectHyperedges(
            hyperedgeDirection = HyperedgeDirection.Both)
        val zip = hyperedges.leftZipJoin(neighbors)(
            (id, hyperedges,neighbors) => (hyperedges, neighbors.getOrElse(0)))

        zip.mapValues(v => (v._1.map(h => h.dstIds.size + h.srcIds.size).sum
                - 1 - -v._2).toDouble / v._2 / (v._1.size - 1))
    }
}
