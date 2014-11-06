package org.apache.spark.hyperx.impl

import org.apache.spark.Logging
import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap

import scala.reflect.ClassTag

/**
 * Each hyperedge is dissembled into multiple (VertexId, HyperedgeId, SrcFlag)
 * tuples. The underlying structure is three big arrays.
 *
 * Additionally, it maintains a source index and a hyperedge index:
 * source index: source VertexId -> array of ids of all the relevant hyperedges
 * hyperedge index: hyperedge id -> the first index in the tuple arrays that
 * corresponds to the hyperedge
 *
 * note: merging data and hIndex will NOT reduce the memory consumption, because
 * Tuple2 has a significant overhead
 *
 */
private[hyperx]
class FlatHyperedgePartition[
@specialized(Int, Long, Double, Byte, Char) ED: ClassTag, VD: ClassTag](
    val vertexIds: Array[VertexId] = null,
    val hyperedgeIds: Array[HyperedgeId] = null,
    val srcFlags: Array[Boolean] = null,
    val data: HyperXOpenHashMap[HyperedgeId, ED] = null,
    val srcIndex: HyperXOpenHashMap[VertexId, Array[HyperedgeId]] = null,
    val hIndex: HyperXOpenHashMap[HyperedgeId, Int] = null,
    val vertices: VertexPartition[VD] = null,
    val activeSet: Option[VertexSet] = None
    ) extends Serializable with Logging {

    val size: Int = hIndex.size

    def withActiveSet(iter: Iterator[VertexId])
    : FlatHyperedgePartition[ED, VD] = {
        val newActiveSet = new VertexSet()
        iter.foreach(newActiveSet.add)
        new FlatHyperedgePartition(vertexIds, hyperedgeIds, srcFlags, data,
            srcIndex, hIndex, vertices, Some(newActiveSet))
    }

    def withActiveSet(set: Option[VertexSet])
    : FlatHyperedgePartition[ED, VD] = {
        new FlatHyperedgePartition(vertexIds, hyperedgeIds, srcFlags, data,
            srcIndex, hIndex, vertices, set)
    }

    def withVertices[VD2: ClassTag](vertices_ : VertexPartition[VD2])
    : FlatHyperedgePartition[ED, VD2] = {
        new FlatHyperedgePartition[ED, VD2](vertexIds, hyperedgeIds, srcFlags, data,
            srcIndex, hIndex, vertices_, activeSet)
    }

    def withData[ED2: ClassTag](data_ : HyperXOpenHashMap[HyperedgeId, ED2])
    : FlatHyperedgePartition[ED2, VD] = {
        new FlatHyperedgePartition(vertexIds, hyperedgeIds, srcFlags,
            data_, srcIndex, hIndex, vertices, activeSet)
    }

    def updateVertices(iter: Iterator[(VertexId, VD)])
    : FlatHyperedgePartition[ED, VD] = {
        this.withVertices[VD](vertices.innerJoinKeepLeft(iter))
    }

    def isActive(vid: VertexId): Boolean ={ activeSet.get.contains(vid)}

    def isActive(vids: VertexSet): Boolean = {
//        vids.iterator.map(isActive).reduce(_ || _)
        val it = vids.iterator
                while (it.hasNext) {
                    if (activeSet.get.contains(it.next())) {
                        return true
                    }
                }
                false
    }

    def numActives: Option[Int] = activeSet.map(_.size)

    def reverse: FlatHyperedgePartition[ED, VD] = {
        val flags = srcFlags.map(!_)
        new FlatHyperedgePartition(vertexIds, hyperedgeIds, flags, data,
            srcIndex, hIndex, vertices, activeSet)
    }

    def map[ED2: ClassTag](f: Hyperedge[ED] => ED2)
    : FlatHyperedgePartition[ED2, VD] = {
        val newData = new HyperXOpenHashMap[HyperedgeId, ED2](data.size)
        val hyperedge = new Hyperedge[ED]()
        val size = vertexIds.size
        var i = 0
        assert(size > 0)
        var currentId = hyperedgeIds(0)
        var srcSet, dstSet = new VertexSet()
        while(i < size) {
            if (currentId != hyperedgeIds(i)) {
                hyperedge.srcIds = srcSet
                hyperedge.dstIds = dstSet
                hyperedge.attr = data(currentId)
                newData(currentId) = f(hyperedge)
                srcSet = new VertexSet()
                dstSet = new VertexSet()
            }
            currentId = hyperedgeIds(i)
            if (srcFlags(i)) {
                srcSet.add(vertexIds(i))
            } else {
                dstSet.add(vertexIds(i))
            }
            i += 1
        }
        // don't forget the last one
        if (srcSet.size > 0 && dstSet.size > 0) {
            hyperedge.srcIds = srcSet
            hyperedge.dstIds = dstSet
            hyperedge.attr = data(currentId)
            newData(currentId) = f(hyperedge)
        }
        this.withData(newData)
    }

    def map[ED2: ClassTag](iter: Iterator[(HyperedgeId, ED2)])
    : FlatHyperedgePartition[ED2, VD] = {
        val seq = iter.toIndexedSeq
        val newData = new HyperXOpenHashMap[HyperedgeId, ED2]()
        seq.foreach{i =>
//            assert(data.hasKey(i._1))
            newData(i._1) = i._2
        }
//        assert(newData.size == data.size)
        this.withData(newData)
    }

    def filter(hpred: HyperedgeTuple[VD, ED] => Boolean,
        vpred: HyperAttr[VD] => Boolean)
    : FlatHyperedgePartition[ED, VD] = {
        val filtered = tupleIterator().filter(t =>
                vpred(t.srcAttr) && vpred(t.dstAttr) && hpred(t)
        )
        val builder = new FlatHyperedgePartitionBuilder[ED, VD]()
        filtered.foreach{h =>
            builder.add(h.srcAttr.keySet, h.dstAttr.keySet,
                h.id, h.attr)
        }
        builder.toFlatHyperedgePartition.withVertices(vertices)
                .withActiveSet(activeSet)
    }

    def tupleIterator(includeSrc: Boolean = true, includeDst: Boolean = true)
    : Iterator[HyperedgeTuple[VD, ED]] = {
        new FlatHyperedgeTupleIterator(this, includeSrc, includeDst)
    }

    def foreach(f: Hyperedge[ED] => Unit): Unit = {
        iterator.foreach(f)
    }

    def iterator = new Iterator[Hyperedge[ED]]{
        private[this] val hyperedge = new Hyperedge[ED]()
        private[this] var index = 0
        private[this] var lastPos = 0

        override def hasNext: Boolean = index < FlatHyperedgePartition.this.size

        // the hyperedgeId is not exposed as it doesn't matter to the outer scope
        override def next(): Hyperedge[ED] = {
            hyperedge.srcIds = new VertexSet()
            hyperedge.dstIds = new VertexSet()
            val pos = hIndex.nextPos(lastPos)
            lastPos = pos + 1
            var i = hIndex._values(pos)
            val currentId = hyperedgeIds(i)
            while(i < hyperedgeIds.size && currentId == hyperedgeIds(i)) {
                val vid = vertexIds(i)
                if (srcFlags(i)) hyperedge.srcIds.add(vid)
                else hyperedge.dstIds.add(vid)
                i += 1
            }
            hyperedge.attr = data(currentId)
            index += 1
            hyperedge
        }
    }

    // todo: srcIds are not sorted, grouping involves a quadratic cost
    def groupHyperedges(merge: (ED, ED) => ED)
    : FlatHyperedgePartition[ED, VD] = {
        val builder = new FlatHyperedgePartitionBuilder[ED, VD]()
        val merged = new Array[(VertexSet, VertexSet)](size)
        var currSrcIds: VertexSet = null.asInstanceOf[VertexSet]
        var currDstIds: VertexSet = null.asInstanceOf[VertexSet]
        var currAttr: ED = null.asInstanceOf[ED]
        val outerIter = iterator
        var i = 0
        while(outerIter.hasNext) {
            val outerH = outerIter.next()
            currSrcIds = outerH.srcIds
            currDstIds = outerH.dstIds
            if (merged.count(r => HyperUtils.is(r._1, currSrcIds) &&
                    HyperUtils.is(r._2, currDstIds)) == 0) {
                currAttr = outerH.attr
//                val outerPos = outerIter.pos
                val innerIter = iterator
//                innerIter.pos = outerPos + 1
                while(innerIter.hasNext) {
                    val inner = innerIter.next()
                    if (HyperUtils.is(inner.srcIds, currSrcIds) &&
                            HyperUtils.is(inner.dstIds, currDstIds)) {
                        currAttr = merge(currAttr, inner.attr)
                    }
                }
                builder.add(currSrcIds, currDstIds, i, currAttr)
                i += 1
            }
        }
        builder.toFlatHyperedgePartition.withVertices(vertices)
                .withActiveSet(activeSet)
    }

    def innerJoin[ED2: ClassTag, ED3: ClassTag](other: FlatHyperedgePartition[ED2, _])
        (f: (VertexSet, VertexSet, ED, ED2) => ED3): FlatHyperedgePartition[ED3, VD] = {
        val builder = new FlatHyperedgePartitionBuilder[ED3, VD]()
        val thisIter = this.iterator
        var i = 0
        while(thisIter.hasNext) {
            val thisH = thisIter.next()
            val otherIter = other.iterator
            while(otherIter.hasNext) {
                val otherH = otherIter.next()
                if (HyperUtils.is(thisH.srcIds, otherH.srcIds) &&
                        HyperUtils.is(thisH.dstIds, otherH.dstIds)) {
                    builder.add(thisH.srcIds, thisH.dstIds, i,
                        f(thisH.srcIds, thisH.dstIds, thisH.attr, otherH.attr))
                }
            }
            i += 1
        }
        builder.toFlatHyperedgePartition.withVertices(vertices)
            .withActiveSet(activeSet)
    }

    def sourceSize: Int = srcIndex.size

    def upgradeIterator(hyperedgeIterator: Iterator[Hyperedge[ED]],
        includeSrc: Boolean = true, includeDst: Boolean = true)
    : Iterator[HyperedgeTuple[VD, ED]] = {
        new ReusingFlatHyperedgeTupleIterator(hyperedgeIterator, this,
            includeSrc, includeDst)
    }

    def indexIterator(srcIdPred: VertexId => Boolean): Iterator[Hyperedge[ED]] = {
        srcIndex.iterator.filter(v => srcIdPred(v._1)).flatMap(each => arrayIndexIterator(each._1))
    }

    private def arrayIndexIterator(srcId: VertexId) = new Iterator[Hyperedge[ED]] {
        private[this] val hyperedge = new Hyperedge[ED]()
        private[this] var pos = 0

        override def hasNext: Boolean = {
            pos >= 0 && pos < srcIndex(srcId).size
        }

        override def next(): Hyperedge[ED] = {
            val hyperedgeId = srcIndex(srcId)(pos)
            var i = hIndex(hyperedgeId)
            hyperedge.srcIds = new VertexSet
            hyperedge.dstIds = new VertexSet
            while(i < hyperedgeIds.size && hyperedgeId == hyperedgeIds(i)) {
                if (srcFlags(i)) hyperedge.srcIds.add(vertexIds(i))
                else hyperedge.dstIds.add(vertexIds(i))
                i += 1
            }
            hyperedge.attr = data(hyperedgeId)
            pos += 1
            hyperedge
        }
    }
}
