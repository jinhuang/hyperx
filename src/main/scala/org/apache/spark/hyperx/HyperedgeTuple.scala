package org.apache.spark.hyperx

import org.apache.spark.hyperx.util.HyperUtils

import scala.reflect.ClassTag

/**
 * A hyperedge tuple represents a hyperedge along with the vertex
 * attributes of all its neighboring vertexes
 *
 * @tparam VD the type of the vertex attribute
 * @tparam ED the type of the hyperedge attribute
 *
 *            Forked from GraphX 2.10, modified by Jin Huang
 */
class HyperedgeTuple[VD: ClassTag, ED: ClassTag] {

    /**
     * The source vertex attributes
     */
    var srcAttr: HyperAttr[VD] = _

    /**
     * The destination vertex attributes
     */
    var dstAttr: HyperAttr[VD] = _


    /**
     * The hyperedge attribute
     */
    var attr: ED = _

    /**
     * Given one vertex set in the edge return the other vertex set attributes
     * @param vids the vertex set one of the two sets on the edge
     * @return the attributes for the other vertex set on the edge
     */
    def otherVertexAttr(vids: VertexSet): HyperAttr[VD] =
        if (HyperUtils.is(srcAttr.keySet, vids)) dstAttr
        else {
            assert(HyperUtils.is(dstAttr.keySet, vids)); srcAttr
        }

    /**
     * Get the vertex attributes for the given vertex set in the edge
     * @param vids the vertex set of one of the two vertex sets on the edge
     * @return the attributes for the vertex set
     */
    def vertexAttr(vids: VertexSet): HyperAttr[VD] =
        if (HyperUtils.is(srcAttr.keySet, vids)) srcAttr
        else {
            assert(HyperUtils.is(dstAttr.keySet, vids)); dstAttr
        }

    override def toString =
        (HyperUtils.mkString(srcAttr, ";"),
        HyperUtils.mkString(dstAttr, ";"),
        attr.toString).toString()

    def toTuple: (HyperAttr[VD], HyperAttr[VD], ED) = (srcAttr, dstAttr, attr)

    /**
     * Set the edge properties of this tuple
     */
    protected[spark] def set(other: Hyperedge[ED]): HyperedgeTuple[VD, ED] = {
        srcAttr = HyperUtils.init[VD](other.srcIds)
        dstAttr = HyperUtils.init[VD](other.dstIds)
        attr = other.attr
        this
    }
}
