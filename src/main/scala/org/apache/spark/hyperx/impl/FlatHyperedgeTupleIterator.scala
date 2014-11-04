package org.apache.spark.hyperx.impl

import org.apache.spark.Logging
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap
import org.apache.spark.hyperx.{Hyperedge, HyperedgeTuple, VertexId}

import scala.reflect.ClassTag

private[impl]
class FlatHyperedgeTupleIterator[VD: ClassTag, ED: ClassTag](
    val hyperedgePartition: FlatHyperedgePartition[ED, VD],
    val includeSrc: Boolean, val includeDst: Boolean)
    extends Iterator[HyperedgeTuple[VD, ED]] with Logging{

    private var index = 0
    private var lastPos = 0

    override def hasNext: Boolean = {
        index < hyperedgePartition.size
    }

    override def next() = {
        val tuple = new HyperedgeTuple[VD, ED]
        tuple.srcAttr = new HyperXOpenHashMap[VertexId, VD]()
        tuple.dstAttr = new HyperXOpenHashMap[VertexId, VD]()
        val pos = hyperedgePartition.hIndex.nextPos(lastPos)
        lastPos = pos + 1
        var i = hyperedgePartition.hIndex._values(pos)
        val currentId = hyperedgePartition.hyperedgeIds(i)
        while(i < hyperedgePartition.hyperedgeIds.size && currentId == hyperedgePartition.hyperedgeIds(i)) {
            val vid = hyperedgePartition.vertexIds(i)
            if (hyperedgePartition.srcFlags(i)) {
                if (includeSrc) {
                    tuple.srcAttr.update(vid, hyperedgePartition.vertices(vid))
                } else {
                    tuple.srcAttr.update(vid, null.asInstanceOf[VD])
                }
            } else {
                if (includeDst) {
                    tuple.dstAttr.update(vid, hyperedgePartition.vertices(vid))
                } else {
                    tuple.dstAttr.update(vid, null.asInstanceOf[VD])
                }
            }
            i += 1
        }
        tuple.id = currentId
        tuple.attr = hyperedgePartition.data(currentId)
        index += 1
        tuple
    }
}


private[impl]
class ReusingFlatHyperedgeTupleIterator[VD: ClassTag, ED: ClassTag](
    val hyperedgeIter: Iterator[Hyperedge[ED]],
    val hyperedgePartition: FlatHyperedgePartition[ED, VD],
    val includeSrc: Boolean, val includeDst: Boolean)
    extends Iterator[HyperedgeTuple[VD, ED]] with Logging{

    private val tuple = new HyperedgeTuple[VD, ED]

    override def hasNext: Boolean = hyperedgeIter.hasNext

    override def next() = {
        tuple.set(hyperedgeIter.next())
        if (includeSrc) {
            tuple.srcAttr.foreach(pair => tuple.srcAttr.update(pair._1,
                hyperedgePartition.vertices(pair._1)))
        }
        if (includeDst) {
            tuple.dstAttr.foreach(pair => tuple.dstAttr.update(pair._1,
                hyperedgePartition.vertices(pair._1)))
        }
        tuple
    }
}