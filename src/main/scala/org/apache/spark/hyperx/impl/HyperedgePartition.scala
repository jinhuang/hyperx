package org.apache.spark.hyperx.impl

import org.apache.spark.Accumulator
import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap

import scala.reflect.{ClassTag, classTag}

/**
 * A collection of hyperedges stored in columnar format,
 * along with any vertex attributes referenced.
 * The hyperedges are stored in 3 large columnar arrays (src, dst,
 * attribute). There is an optional
 * active vertex set for filtering the computation on hyperedges.
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */
private[hyperx]
class HyperedgePartition[
@specialized(Char, Int, Boolean, Byte, Long, Float, Double)
    ED: ClassTag,VD: ClassTag](
    val srcIds: Array[VertexSet] = null,
    val dstIds: Array[VertexSet] = null,
    val data: Array[ED] = null,
    val srcIndex: HyperXOpenHashMap[VertexId, Array[Int]] = null,
    val vertices: VertexPartition[VD] = null,
    val activeSet: Option[VertexSet] = None
    ) extends Serializable {

    /** The number of hyperedges in this partition. */
    val size: Int = srcIds.size

    /** Return a new `HyperedgePartition` with the specified active set,
      * provided as an iterator. */
    def withActiveSet(iter: Iterator[VertexId]): HyperedgePartition[ED, VD] = {
        val activeArray = iter.toArray
        val newActiveSet: VertexSet = new VertexSet()
        activeArray.foreach(newActiveSet.add)
        new HyperedgePartition(srcIds, dstIds, data, srcIndex, vertices,
            Some(newActiveSet))
    }

    /** Return a new `HyperedgePartition` with the specified active set. */
    def withActiveSet(activeSet_ : Option[VertexSet]): HyperedgePartition[ED,
            VD] = {
        new HyperedgePartition(srcIds, dstIds, data, srcIndex, vertices,
            activeSet_)
    }

    /** Return a new `HyperedgePartition` with updates to vertex attributes
      * specified in `iter`. */
    def updateVertices(iter: Iterator[(VertexId,VD)]): HyperedgePartition[ED, VD] = {
        this.withVertices(vertices.innerJoinKeepLeft(iter))
    }

    /** Look up vid in activeSet, throwing an exception if it is None. */
    def isActive(vid: VertexId): Boolean = {
        activeSet.get.contains(vid)
    }

    /** Look up vids in activeSet, throwing an exception if it is None. */
    def isActive(vids: VertexSet): Boolean = {
        val it = vids.iterator
        while (it.hasNext) {
            if (activeSet.get.contains(it.next())) {
                return true
            }
        }
        false
    }

    /** The number of active vertices, if any exists */
    def numActives: Option[Int] = activeSet.map(_.size)

    /**
     * Reverse all the hyperedges in this partition
     * @return a new hyperedge partition with all the hyperedges reversed
     */
    def reverse: HyperedgePartition[ED, VD] = {
        val builder = new HyperedgePartitionBuilder(size)(classTag[ED],
            classTag[VD])
        for (h <- iterator) {
            builder.add(h.dstIds, h.srcIds, h.attr)
        }
        builder.toHyperedgePartition.withVertices(vertices)
                .withActiveSet(activeSet)
    }

    /**
     * Construct a new hyperedge partition by applying the function f to all
     * hyperedges
     * in this partition
     *
     * @param f a function from an hyperedge to a new attribute
     * @tparam ED2 the type of the new attribute
     * @return a new hyperedge partition with the result of the function `f`
     *         applied to each edge
     */
    def map[ED2: ClassTag](f: Hyperedge[ED] => ED2): HyperedgePartition[ED2,
            VD] = {
        val newData = new Array[ED2](data.size)
        val hyperedge = new Hyperedge[ED]()
        val size = data.size
        var i = 0
        while (i < size) {
            hyperedge.srcIds = srcIds(i)
            hyperedge.dstIds = dstIds(i)
            hyperedge.attr = data(i)
            newData(i) = f(hyperedge)
            i += 1
        }
        this.withData(newData)
    }

    /**
     * Construct a new hyperedge partition by using the edge attribute
     * contained in the iterator
     *
     * @note The input iterator should return hyperedge attributes in the
     *       order of
     *       the hyperedges returned by `HyperedgePartition.iterator` and
     *       should return
     *       attributes equal to the number of hyperedges
     *
     * @param iter an iterator for the new attribute values
     * @tparam ED2 the type of new attributes
     * @return a new hyperedge partition with the attribute values replaced
     */
    def map[ED2: ClassTag](iter: Iterator[ED2]): HyperedgePartition[ED2, VD] = {
        val newData = new Array[ED2](data.size)
        var i = 0
        while (iter.hasNext) {
            newData(i) = iter.next()
            i += 1
        }
        assert(newData.size == i)
        this.withData(newData)
    }

    /** Return a new `HyperedgePartition` with the specified hyperedge data. */
    def withData[ED2: ClassTag](data_ : Array[ED2]): HyperedgePartition[ED2,
            VD] = {
        new HyperedgePartition(srcIds, dstIds, data_, srcIndex, vertices,
            activeSet)
    }

    /**
     * Constrcut a new hyperedge partition containing only the hyperedge
     * matching `epred`
     * and where both vertex sets match `vpred`
     */
    def filter(
                      epred: HyperedgeTuple[VD, ED] => Boolean,
                      vpred: HyperAttr[VD] => Boolean):
    HyperedgePartition[ED, VD] = {
        val filtered = tupleIterator().filter(et =>
            vpred(et.srcAttr) && vpred(et.dstAttr) && epred(et))
        val builder = new HyperedgePartitionBuilder[ED, VD]
        for (e <- filtered) {
            builder.add(e.srcAttr.keySet, e.dstAttr.keySet, e.attr)
        }
        builder.toHyperedgePartition.withVertices(vertices)
                .withActiveSet(activeSet)
    }

    /**
     * Get an iterator over the hyperedge tuple in this partition
     *
     * It is safe to keep references to the objects from this iterator
     */
    def tupleIterator(includeSrc: Boolean = true, includeDst: Boolean = true)
    : Iterator[HyperedgeTuple[VD, ED]] =
        new HyperedgeTupleIterator(this, includeSrc, includeDst)

    /**
     * Apply the function `f` to all hyperedges in this partition
     */
    def foreach(f: Hyperedge[ED] => Unit): Unit = {
        iterator.foreach(f)
    }

    /**
     * Get an iterator over the hyperedges in this partition
     *
     * Be careful not to keep references to the objects from this iterator.
     *
     * @return an iterator over hyperedges in the partition
     */
    def iterator = new Iterator[Hyperedge[ED]] {
        private[this] val hyperedge = new Hyperedge[ED]()
        private[this] var pos = 0

        override def hasNext: Boolean = pos < HyperedgePartition.this.size

        override def next(): Hyperedge[ED] = {
            hyperedge.srcIds = srcIds(pos)
            hyperedge.dstIds = dstIds(pos)
            hyperedge.attr = data(pos)
            pos += 1
            hyperedge
        }
    }

    /**
     * Merge all the hyperedges with the same src and dest set into a single
     * hyperedge
     * using the `merge` function
     */
    def groupHyperedges(merge: (ED, ED) => ED): HyperedgePartition[ED, VD] = {
        val builder = new HyperedgePartitionBuilder[ED, VD]
        val currSrcIds: VertexSet = null.asInstanceOf[VertexSet]
        val currDstIds: VertexSet = null.asInstanceOf[VertexSet]
        var currAttr: ED = null.asInstanceOf[ED]
        var i = 0
        while (i < size) {
            if (i > 0 && HyperUtils.is(currSrcIds, srcIds(i)) && HyperUtils
                    .is(currDstIds, dstIds(i))) {
                currAttr = merge(currAttr, data(i))
            }
            else {
                if (i > 0) {
                    builder.add(currSrcIds, currDstIds, currAttr)
                }
            }
            i += 1
        }
        if (size > 0) {
            builder.add(currSrcIds, currDstIds, currAttr)
        }
        builder.toHyperedgePartition.withVertices(vertices)
                .withActiveSet(activeSet)
    }

    /**
     * Apply `f` to all hyperedges present in both `this` and `other` and
     * return new `HyperedgePartition`
     * containing the resulting hyperedges
     *
     * This is rather costly, as we don't have any clustered index on neither
     * partition.
     * The join is carried out via a naive cartesian product
     * @todo To implement a more efficient join algorithm
     **/
    def innerJoin[ED2: ClassTag, ED3: ClassTag]
    (other: HyperedgePartition[ED2, _])
    (f: (VertexSet, VertexSet, ED, ED2) => ED3): HyperedgePartition[ED3, VD] = {
        val builder = new HyperedgePartitionBuilder[ED3, VD]
        var i = 0
        var j = 0
        while (i < this.size) {
            while (j < this.size) {
                if (HyperUtils.is(srcIds(i), other.srcIds(j)) && HyperUtils
                        .is(dstIds(i), other.dstIds(j))) {
                    builder.add(srcIds(i), dstIds(i), f(srcIds(i), dstIds(i),
                        this.data(i), other.data(j)))
                }
                j += 1
            }
            i += 1
        }
        builder.toHyperedgePartition.withVertices(vertices)
                .withActiveSet(activeSet)
    }

    /** Return a new `HyperedgePartition` with the specified vertex partition
      * . */
    def withVertices[VD2: ClassTag](vertices_ : VertexPartition[VD2]):
    HyperedgePartition[ED, VD2] = {
        new HyperedgePartition(srcIds, dstIds, data, srcIndex, vertices_,
            activeSet)
    }

    /** The number of unique source vertices in the partition. */
    def indexSize: Int = srcIndex.size

    /**
     * Upgrade the given hyperedge iterator into a tuple iterator
     *
     * Be careful not to keep references to the objects from this iterator.
     */
    def upgradeIterator(
       hyperedgeIter: Iterator[Hyperedge[ED]],
       includeSrc: Boolean = true,
       includeDst: Boolean = true)
    : Iterator[HyperedgeTuple[VD, ED]] = {
        new ReusingHyperedgeTupleIterator(hyperedgeIter, this, includeSrc, includeDst)
    }

    def upgradeIteratorP(
        hyperedgeIter: Iterator[Hyperedge[ED]],tracker: Accumulator[Int],
        includeSrc: Boolean = true,
        includeDst: Boolean = true)
    : Iterator[HyperedgeTuple[VD, ED]] = {
        val start = System.currentTimeMillis()
        val ret = new ReusingHyperedgeTupleIterator(hyperedgeIter, this, includeSrc, includeDst)
        tracker += (System.currentTimeMillis() - start).toInt
        ret
    }

    /**
     * Get an iterator over the hyperedges in this partition whose source vertex id match
     * srcIdsPred. The iterator is generated using an index scan, so it is efficient at skipping
     * edges that don't match srcIdsPred.
     *
     * Be careful not to keep references to the objects from this iterator.
     */
    def indexIterator(srcIdPred: VertexId => Boolean): Iterator[Hyperedge[ED]] = {
        srcIndex.iterator.filter(kv => srcIdPred(kv._1)).flatMap(each => arrayIndexIterator(each._1))
    }

    /**
     * Get an iterator over the array index of hyperedges in this partition with source vertex
     * id `srcId`
     *
     * Be careful not to keep references to the objects from this iterator.
     * @param srcId the source vertex id
     * @return an iterator over the hyperedges that has source vertex `srcId`
     */
    private def arrayIndexIterator(srcId: VertexId) = new Iterator[Hyperedge[ED]] {
        private[this] val hyperedge = new Hyperedge[ED]()
        private[this] var pos = 0

        override def hasNext: Boolean = {
            pos >= 0 && pos < HyperedgePartition.this.srcIndex(srcId).size
        }

        override def next(): Hyperedge[ED] = {
            assert(srcIds(srcIndex(srcId)(pos)).contains(srcId))
            hyperedge.srcIds = srcIds(srcIndex(srcId)(pos))
            hyperedge.dstIds = dstIds(srcIndex(srcId)(pos))
            hyperedge.attr = data(srcIndex(srcId)(pos))
            pos += 1
            hyperedge
        }
    }
}
