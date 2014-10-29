package org.apache.spark.hyperx.impl

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.{Hyperedge, HyperedgeTuple}

import scala.reflect.ClassTag

/**
 * The Iterator type returned when constructing hyperedge tuples.
 */
private[impl]
class HyperedgeTupleIterator[VD: ClassTag, ED: ClassTag](
    val hyperedgePartition: HyperedgePartition[ED, VD], val includeSrc: Boolean,
    val includeDst: Boolean)
    extends Iterator[HyperedgeTuple[VD, ED]] {

    private var pos = 0

    override def hasNext: Boolean = pos < hyperedgePartition.size

    override def next() = {
        val tuple = new HyperedgeTuple[VD, ED]
        tuple.srcAttr = HyperUtils.init(hyperedgePartition.srcIds(pos))
        if (includeSrc) {
            for (it <- tuple.srcAttr.iterator) {
                tuple.srcAttr.update(it._1, hyperedgePartition.vertices(it._1))
            }
        }
        tuple.dstAttr = HyperUtils.init(hyperedgePartition.dstIds(pos))
        if (includeDst) {
            for (it <- tuple.dstAttr.iterator) {
                tuple.dstAttr.update(it._1, hyperedgePartition.vertices(it._1))
            }
        }
        tuple.attr = hyperedgePartition.data(pos)
        pos += 1
        tuple
    }
}

/**
 * An Iterator type for internal use that reuses HyperedgeTuple objects
 */
private[impl]
class ReusingHyperedgeTupleIterator[VD: ClassTag, ED: ClassTag](
    val hyperedgeIter: Iterator[Hyperedge[ED]],
    val hyperedgePartition: HyperedgePartition[ED, VD],
    val includeSrc: Boolean, val includeDst: Boolean) extends
    Iterator[HyperedgeTuple[VD, ED]] {

    private val tuple = new HyperedgeTuple[VD, ED]

    override def hasNext = hyperedgeIter.hasNext

    override def next() = {
        tuple.set(hyperedgeIter.next())
        if (includeSrc) {
            for (it <- tuple.srcAttr.iterator) {
                tuple.srcAttr.update(it._1, hyperedgePartition.vertices(it._1))
            }
        }
        if (includeDst) {
            for (it <- tuple.dstAttr.iterator) {
                tuple.dstAttr.update(it._1, hyperedgePartition.vertices(it._1))
            }
        }
        tuple
    }
}
