package org.apache.spark

import org.apache.spark.hyperx.util.collection.{HyperXOpenHashMap, HyperXOpenHashSet}

/**
 * <span class="badge" style="float: right;">ALPHA COMPONENT</span>
 * HyperX is a hypergraph processing framework built on top of Spark and
 * extending GraphX
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */

package object hyperx {

    /**
     * A 64-bit vertex identifier that uniquely identifies a vertex within a
     * hypergraph.
     * It does not need to follow any ordering or any constraints other than
     * uniqueness.
     */
//    type VertexId = Long

    type VertexId = Long

    type HyperedgeId = Int

    /** Integer identifier of a hypergraph partition. */
    type PartitionId = Int

    /** A hash set containing a collection of vertex identifiers */
//    private[hyperx] type VertexSet = OpenHashSet[VertexId]

    private[hyperx] type VertexSet = HyperXOpenHashSet[VertexId]

    /** A hash map containing a collection of vertex identifier and
      * corresponding attributes */
    private[hyperx] type HyperAttr[VD] = HyperXOpenHashMap[VertexId, VD]
}
