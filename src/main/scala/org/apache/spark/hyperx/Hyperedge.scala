
package org.apache.spark.hyperx

import org.apache.spark.hyperx.util.HyperUtils

/**
 * A single directed hyperedge consisting of a set of source ids, a set of
 * target ids, and the data associated with the hyperedge
 *
 * @tparam ED type of the hyperedge attribute
 *
 *            Forked from GraphX, modified by Jin Huang
 */
case class Hyperedge[@specialized(Char, Int, Boolean, Byte, Long, Float,
    Double) ED](var srcIds: VertexSet,var dstIds: VertexSet,
                var attr: ED = null.asInstanceOf[ED])
        extends Serializable {

    var id: HyperedgeId = null.asInstanceOf[HyperedgeId]

    def this() = {
        this(null.asInstanceOf[VertexSet], null.asInstanceOf[VertexSet])
    }

    /**
     * Given one vertex set in the hyperedge return the other hyperedge
     * @param vids the vertex set one of the two sets on the edge
     * @return the set of the other vertex set on the edge
     */
    def otherVertexIds(vids: VertexSet): VertexSet =
        if (HyperUtils.is(srcIds, vids)) dstIds
        else {
            assert(HyperUtils.is(dstIds, vids))
            srcIds
        }

    def relativeDirection(vids: VertexSet): HyperedgeDirection =
        if (HyperUtils.is(srcIds, vids)) HyperedgeDirection.Out
        else {
            assert(HyperUtils.is(dstIds, vids))
            HyperedgeDirection.In
        }
}
