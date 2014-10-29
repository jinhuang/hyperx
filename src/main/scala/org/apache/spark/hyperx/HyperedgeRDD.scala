package org.apache.spark.hyperx

import org.apache.spark.hyperx.impl.{HyperedgePartition, HyperedgePartitionBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, Partitioner, TaskContext}

import scala.reflect.{ClassTag, classTag}

/** Forked from GraphX, modified by Jin Huang */
class HyperedgeRDD[@specialized ED: ClassTag, VD: ClassTag](
        val partitionsRDD: RDD[(PartitionId, HyperedgePartition[ED, VD])],
        val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
        extends RDD[Hyperedge[ED]](partitionsRDD.context,
            List(new OneToOneDependency(partitionsRDD))) {

    partitionsRDD.setName("HyperedgeRDD")

    /**
     * Use the partitioner of the embedded RDD, if couldn't find one,
     * use the default partitioner
     */
    override val partitioner =
        partitionsRDD.partitioner.orElse(Some(Partitioner.defaultPartitioner
                (partitionsRDD)))

    override def compute(part: Partition, context: TaskContext):
    Iterator[Hyperedge[ED]] = {
        val p = firstParent[(PartitionId, HyperedgePartition[ED,
                VD])].iterator(part, context)
        if (p.hasNext) {
            p.next()._2.iterator.map(_.copy())
        } else {
            Iterator.empty
        }
    }

    override def collect(): Array[Hyperedge[ED]] = this.map[Hyperedge[ED]](h
    => h.copy()) collect()

    override def persist(newLevel: StorageLevel): this.type = {
        partitionsRDD.persist(newLevel)
        this
    }

    override def unpersist(blocking: Boolean = true): this.type = {
        partitionsRDD.unpersist(blocking)
        this
    }

    /** Persists the vertex partitions using `targetStorageLevel`,
      * which defaults to MEMORY_ONLY */
    override def cache(): this.type = {
        partitionsRDD.persist(targetStorageLevel)
        this
    }

    /**
     * Map the values in a hyperedge partitioning preserving the structure
     * but changing the values.
     * @param f the function from a hyperedge to a new hyperedge attribute
     * @tparam ED2 the new hyperedge attribute type
     * @return a new HyperedgeRDD containing the new hyperedge attributes
     */
    def mapValues[ED2: ClassTag](f: Hyperedge[ED] => ED2): HyperedgeRDD[ED2,
            VD] = {
        mapHyperedgePartitions((pid, part) => part.map(f))
    }

    /**
     * Reverse all the hyperedges in this RDD
     * @return a new HyperedgeRDD containing all the hyperedge reversed
     */
    def reverse: HyperedgeRDD[ED, VD] = mapHyperedgePartitions((pid,
                                                                part) => part
            .reverse)

    /** Applies a function on each hyperedge partition,
      * and produces a new HyperedgeRDD with new partitions */
    private[hyperx] def mapHyperedgePartitions[ED2: ClassTag, VD2: ClassTag](
        f: (PartitionId, HyperedgePartition[ED, VD]) =>
                HyperedgePartition[ED2, VD2]): HyperedgeRDD[ED2, VD2] = {
        this.withPartitionsRDD[ED2, VD2](partitionsRDD.mapPartitions({ iter =>
            if (iter.hasNext) {
                val (pid, ep) = iter.next()
                Iterator(Tuple2(pid, f(pid, ep)))
            } else {
                Iterator.empty
            }
        }, preservesPartitioning = true))
    }

    /**
     * Removen all hyperedges but those matching `hpred` and where both
     * vertices match `vpred`
     * @param hpred the predicate on hyperedges
     * @param vpred the predicate on vertices
     */
    def filter(hpred: HyperedgeTuple[VD, ED] => Boolean,
               vpred: HyperAttr[VD] => Boolean): HyperedgeRDD[ED, VD] = {
        mapHyperedgePartitions((pid, part) => part.filter(hpred, vpred))
    }

    /**
     * Inner joins this HyperedgeRDD with another HyperedgeRDD,
     * assuming both are partitioned using the same
     * PartitionStrategy
     *
     * This join is not efficient as the underlying joins between two
     * HyperedgePartitions
     * are carried out via a brutal cartesian product.
     *
     * @todo implement a more efficient join algorithm
     *
     * @param other the other HyperedgeRDD to join with
     * @param f the join function applied to corresponding values of `this`
     *          and `other`
     * @tparam ED2 the attribute type of hyperedges in `other`
     * @tparam ED3 the attribute type of the joined hyperedges
     * @return a new HyperedgeRDD containing only hyperedges that appear in
     *         both `this`
     *         and `other` with values supplied by `f`
     */
    def innerJoin[ED2: ClassTag, ED3: ClassTag]
    (other: HyperedgeRDD[ED2, _])
    (f: (VertexSet, VertexSet, ED, ED2) => ED3) = {
        val ed2Tag = classTag[ED2]
        val ed3Tag = classTag[ED3]
        this.withPartitionsRDD[ED3, VD](partitionsRDD.zipPartitions(other
                .partitionsRDD, preservesPartitioning = true) {
            (thisIter, otherIter) =>
                val (pid, thisEPart) = thisIter.next()
                val (_, otherEPart) = otherIter.next()
                Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)
                        (ed2Tag, ed3Tag)))
        })
    }

    /** Replaces the vertex partitions while preserving all other properties
      * of the VertexRDD. */
    private[hyperx] def withPartitionsRDD[ED2: ClassTag, VD2: ClassTag](
        partitionsRDD: RDD[(PartitionId, HyperedgePartition[ED2, VD2])])
    : HyperedgeRDD[ED2, VD2] = {
        new HyperedgeRDD(partitionsRDD, this.targetStorageLevel)
    }

    override protected def getPartitions: Array[Partition] = partitionsRDD
            .partitions

    /** Changes the target storage level while preserving all other
      * properties of the
      * HyperedgeRDD. Operations on the returned HyperedgeRDD will preserve
      * this storage
      * level. */
    private[hyperx] def withTargetStorageLevel(tagrteStorageLevel:StorageLevel)
    :HyperedgeRDD[ED, VD] = {
        new HyperedgeRDD(this.partitionsRDD, targetStorageLevel)
    }
}

object HyperedgeRDD {

    /**
     * Create a HyperedgeRDD from a set (RDD) of hyperedges.
     * @param hyperedges the input hyperedges
     * @tparam ED the type of hyperedge attribute
     * @tparam VD the type of vertex attribute
     * @return a new HyperedgeRDD created for the input hyperedges
     */
    def fromHyperedges[ED: ClassTag, VD: ClassTag](hyperedges:
                                                   RDD[Hyperedge[ED]]):
    HyperedgeRDD[ED, VD] = {
        val hyperedgePartitions = hyperedges.mapPartitionsWithIndex{
            (pid,iter) =>
            val builder = new HyperedgePartitionBuilder[ED, VD]
            iter.foreach { e =>
                builder.add(e.srcIds, e.dstIds, e.attr)
            }
            Iterator((pid, builder.toHyperedgePartition))
        }
        HyperedgeRDD.fromHyperedgePartitions(hyperedgePartitions)
    }

    /**
     * Create a HyperedgeRDD from previously constructed hyperedge partitions.
     * @param hyperedgePartitions the previously constructed hyperedge partitions
     * @tparam ED the type of hyperedge attribute
     * @tparam VD the type of vertex attribute
     * @return a new HyperedgeRDD created for the hyperedge partitions
     */
    def fromHyperedgePartitions[ED: ClassTag, VD: ClassTag](
        hyperedgePartitions: RDD[(Int, HyperedgePartition[ED, VD])],
        storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : HyperedgeRDD[ED, VD] = {
        new HyperedgeRDD(hyperedgePartitions, storageLevel)
    }
}