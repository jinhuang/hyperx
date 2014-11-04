package org.apache.spark.hyperx

import org.apache.spark._
import org.apache.spark.hyperx.impl.VertexRDDFunctions._
import org.apache.spark.hyperx.impl.{RoutingTablePartition, ShippableVertexPartition, VertexAttributeBlock}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Extends `RDD[(VertexId, VD)]` by ensuring that their is only one entry for
 * each vertex.
 * By pre-indexing the entries for fast, efficient joins. Two VertexRDDs with
 * the same index
 * can be joined efficiently. All operations except reindex preserve the
 * index. To construct
 * a `VertexRDD`, use the [[org.apache.spark.hyperx.VertexRDD]]$ VertexRDD
 * object.
 *
 * Additionally, stores routing information to enable joining the vertex
 * attributes with an
 * [[HyperedgeRDD]]
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */
class VertexRDD[@specialized VD: ClassTag](
    val partitionsRDD: RDD[ShippableVertexPartition[VD]],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    extends RDD[(VertexId, VD)](partitionsRDD.context,
    List(new OneToOneDependency(partitionsRDD))) {

//    require(partitionsRDD.partitioner.isDefined)

    override val partitioner = Option(partitionsRDD.partitioner.getOrElse(new HashPartitioner(partitionsRDD.partitions.size)))

    /**
     * Construct a new VertexRDD that is indexed by only the visible vertices
     * . The resulting
     * VertexRDD will be based on a different index and can no longer be
     * quickly joined with this
     * RDD.
     */
    def reindex(): VertexRDD[VD] =  this.withPartitionsRDD(partitionsRDD.map(_.reindex()))

    override def setName(_name: String): this.type = {
        if (partitionsRDD.name != null) {
            partitionsRDD.setName(partitionsRDD.name + ", " + _name)
        } else {
            partitionsRDD.setName(_name)
        }
        this
    }

    /**
     * Persists the vertex partitions at the specified storage level,
     * ignoring any existing target
     * storage level.
     */
    override def persist(newLevel: StorageLevel): this.type = {
        partitionsRDD.persist(newLevel)
        this
    }

    override def unpersist(blocking: Boolean = true): this.type = {
        partitionsRDD.unpersist(blocking)
        this
    }

    setName("VertexRDD")

    /** Persists the vertex partitions at `targetStorageLevel`,
      * which defaults to MEMORY_ONLY. */
    override def cache(): this.type = {
        partitionsRDD.persist(targetStorageLevel)
        this
    }

    /** The number of vertices in the RDD. */
    override def count(): Long = {
        partitionsRDD.map(_.size).reduce(_ + _)
    }

    /**
     * Provides the `RDD[(VertexId, VD)]` equivalent output.
     */
    override def compute(part: Partition, context: TaskContext)
    : Iterator[(VertexId, VD)] = {
        firstParent[ShippableVertexPartition[VD]].iterator(part,
            context).next().iterator
    }

    /**
     * Restricts the vertex set to the set of vertices satisfying the given
     * predicate. This operation
     * preserves the index for efficient joins with the original RDD,
     * and it sets bits in the bitmask
     * rather than allocating new memory.
     *
     * @param pred the user defined predicate, which takes a tuple to conform
     *             to the
     *             `RDD[(VertexId, VD)]` interface
     */
    override def filter(pred: ((VertexId, VD)) => Boolean): VertexRDD[VD] =
        this.mapVertexPartitions(_.filter(Function.untupled(pred)))

    /**
     * Maps each vertex attribute, preserving the index.
     *
     * @tparam VD2 the type returned by the map function
     *
     * @param f the function applied to each value in the RDD
     * @return a new VertexRDD with values obtained by applying `f` to each
     *         of the entries in the
     *         original VertexRDD
     */
    def mapValues[VD2: ClassTag](f: VD => VD2): VertexRDD[VD2] =
        this.mapVertexPartitions(_.map((vid, attr) => f(attr)))

    /**
     * Maps each vertex attribute, additionally supplying the vertex ID.
     *
     * @tparam VD2 the type returned by the map function
     *
     * @param f the function applied to each ID-value pair in the RDD
     * @return a new VertexRDD with values obtained by applying `f` to each
     *         of the entries in the
     *         original VertexRDD.  The resulting VertexRDD retains the same
     *         index.
     */
    def mapValues[VD2: ClassTag](f: (VertexId, VD) => VD2): VertexRDD[VD2] =
        this.mapVertexPartitions(_.map(f))

    /**
     * Hides vertices that are the same between `this` and `other`; for
     * vertices that are different,
     * keeps the values from `other`.
     */
    def diff(other: VertexRDD[VD]): VertexRDD[VD] = {
        val newPartitionsRDD = partitionsRDD.zipPartitions(
            other.partitionsRDD, preservesPartitioning = true
        ) { (thisIter, otherIter) =>
            val thisPart = thisIter.next()
            val otherPart = otherIter.next()
            Iterator(thisPart.diff(otherPart))
        }
        this.withPartitionsRDD(newPartitionsRDD)
    }

    /**
     * Left joins this VertexRDD with an RDD containing vertex attribute
     * pairs. If the other RDD is
     * backed by a VertexRDD with the same index then the efficient
     * [[leftZipJoin]] implementation is
     * used. The resulting VertexRDD contains an entry for each vertex in
     * `this`. If `other` is
     * missing any vertex in this VertexRDD, `f` is passed `None`. If there
     * are duplicates,
     * the vertex is picked arbitrarily.
     *
     * @tparam VD2 the attribute type of the other VertexRDD
     * @tparam VD3 the attribute type of the resulting VertexRDD
     *
     * @param other the other VertexRDD with which to join
     * @param f the function mapping a vertex id and its attributes in this
     *          and the other vertex set
     *          to a new vertex attribute.
     * @return a VertexRDD containing all the vertices in this VertexRDD with
     *         the attributes emitted
     *         by `f`.
     */
    def leftJoin[VD2: ClassTag, VD3: ClassTag]
        (other: RDD[(VertexId, VD2)])
        (f: (VertexId, VD, Option[VD2]) => VD3)
    : VertexRDD[VD3] = {
        // Test if the other vertex is a VertexRDD to choose the optimal join
        // strategy.
        // If the other set is a VertexRDD then we use the much more
        // efficient leftZipJoin
        other match {
            case other: VertexRDD[VD2] =>
                leftZipJoin[VD2, VD3](other)(f)
            case _ =>
                this.withPartitionsRDD[VD3](
                    partitionsRDD.zipPartitions(
                        other.copartitionWithVertices(this.partitioner.get),
                        preservesPartitioning = true)(f = (partIter, msgs) =>
                        partIter.map(_.leftJoin(msgs)(f)))
                )
        }
    }

    /**
     * Left joins this RDD with another VertexRDD with the same index. This
     * function will fail if
     * both VertexRDDs do not share the same index. The resulting vertex set
     * contains an entry for
     * each vertex in `this`.
     * If `other` is missing any vertex in this VertexRDD, `f` is passed `None`.
     *
     * @tparam VD2 the attribute type of the other VertexRDD
     * @tparam VD3 the attribute type of the resulting VertexRDD
     *
     * @param other the other VertexRDD with which to join.
     * @param f the function mapping a vertex id and its attributes in this
     *          and the other vertex set
     *          to a new vertex attribute.
     * @return a VertexRDD containing the results of `f`
     */
    def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
    (other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3):
    VertexRDD[VD3] = {
        val newPartitionsRDD = partitionsRDD.zipPartitions(
            other.partitionsRDD, preservesPartitioning = true
        ) { (thisIter, otherIter) =>
            val thisPart = thisIter.next()
            val otherPart = otherIter.next()
            Iterator(thisPart.leftJoin(otherPart)(f))
        }
        this.withPartitionsRDD(newPartitionsRDD)
    }

    /**
     * Inner joins this VertexRDD with an RDD containing vertex attribute
     * pairs. If the other RDD is
     * backed by a VertexRDD with the same index then the efficient
     * [[innerZipJoin]] implementation
     * is used.
     *
     * @param other an RDD containing vertices to join. If there are multiple
     *              entries for the same
     *              vertex, one is picked arbitrarily. Use
     *              [[aggregateUsingIndex]] to merge multiple entries.
     * @param f the join function applied to corresponding values of `this`
     *          and `other`
     * @return a VertexRDD co-indexed with `this`, containing only vertices
     *         that appear in both
     *         `this` and `other`, with values supplied by `f`
     */
    def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
        (f: (VertexId, VD, U) => VD2):VertexRDD[VD2] = {
        other match {
            case other: VertexRDD[U] =>
                innerZipJoin[U, VD2](other)(f)
            case _ =>
                this.withPartitionsRDD {
                    partitionsRDD.zipPartitions(
                        other.copartitionWithVertices(this.partitioner.get),
                        preservesPartitioning = true) {
                        (partIter, msgs) =>
                            partIter.map(_.innerJoin(msgs)(f))
                    }
                }
        }
    }

    /**
     * Efficiently inner joins this VertexRDD with another VertexRDD sharing
     * the same index. See
     * innerJoin for the behavior of the join.
     */
    def innerZipJoin[U: ClassTag, VD2: ClassTag]
    (other: VertexRDD[U])
    (f: (VertexId, VD, U) => VD2): VertexRDD[VD2] = {
        val newPartitionsRDD = partitionsRDD.zipPartitions(
            other.partitionsRDD, preservesPartitioning = true
        ) { (thisIter, otherIter) =>
            val thisPart = thisIter.next()
            val otherPart = otherIter.next()
            Iterator(thisPart.innerJoin(otherPart)(f))
        }
        this.withPartitionsRDD(newPartitionsRDD)
    }

    /**
     * Aggregates vertices in `messages` that have the same ids using
     * `reduceFunc`, returning a
     * VertexRDD co-indexed with `this`.
     *
     * @param messages an RDD containing messages to aggregate,
     *                 where each message is a pair of its
     *                 target vertex ID and the message data
     * @param reduceFunc the associative aggregation function for merging
     *                   messages to the same vertex
     * @return a VertexRDD co-indexed with `this`, containing only vertices
     *         that received messages.
     *         For those vertices, their values are the result of applying
     *         `reduceFunc` to all received
     *         messages.
     */
    def aggregateUsingIndex[VD2: ClassTag](
        messages: RDD[(VertexId,VD2)],
        reduceFunc: (VD2,VD2) => VD2):
    VertexRDD[VD2] = {
        // shuffle messages to their destinations
        val shuffled = messages.copartitionWithVertices(this.partitioner.get)
        // combine the messages sent to the same vertices
        val parts = partitionsRDD.zipPartitions(shuffled,
            preservesPartitioning = true) { (thisIter, msgIter) =>
                thisIter.map(_.aggregateUsingIndex(msgIter, reduceFunc))
        }
        this.withPartitionsRDD[VD2](parts)
    }

    def aggregateUsingIndexP[VD2: ClassTag](messages: RDD[(VertexId,VD2)],
        reduceFunc: (VD2,VD2) => VD2, rT: Array[Accumulator[Int]], rStart: Array[Accumulator[Long]], rCpl: Array[Accumulator[Long]], countAcc: Array[Accumulator[Int]])
    : VertexRDD[VD2] = {
        // shuffle messages to their destinations
        val shuffled = messages.copartitionWithVertices(this.partitioner.get)
        // combine the messages sent to the same vertices
        val parts = partitionsRDD.zipWithIndex().zipPartitions(shuffled,
            preservesPartitioning = true) { (thisIter, msgIter) =>
            thisIter.map{p =>
                val ret = p._1.aggregateUsingIndexCP(msgIter, reduceFunc, rT(p._2.toInt), rStart(p._2.toInt), rCpl(p._2.toInt), countAcc(p._2.toInt))
                ret
            }
        }
        val ret = this.withPartitionsRDD[VD2](parts)
        ret
    }

    /**
     * Returns a new `VertexRDD` reflecting a reversal of all edge directions
     * in the corresponding
     * [[HyperedgeRDD]].
     */
    def reverseRoutingTables(): VertexRDD[VD] =
        this.mapVertexPartitions(vPart => vPart.withRoutingTable(vPart
                .routingTable.reverse))

    /**
     * Applies a function to each `VertexPartition` of this RDD and returns a
     * new VertexRDD.
     */
    private[hyperx] def mapVertexPartitions[VD2: ClassTag](f:
        ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])
    : VertexRDD[VD2] = {
        val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f),
            preservesPartitioning = true)
        this.withPartitionsRDD(newPartitionsRDD)
    }

    /** Replaces the vertex partitions while preserving all other properties
      * of the VertexRDD. */
    private[hyperx] def withPartitionsRDD[VD2: ClassTag](
        partitionsRDD: RDD[ShippableVertexPartition[VD2]]): VertexRDD[VD2] = {
        new VertexRDD(partitionsRDD, this.targetStorageLevel)
    }

    /** Prepare this VertexRDD for efficient joins with the given HyperedgeRDD
      * . */
    def withHyperedges(hyperedges: HyperedgeRDD[_, _]): VertexRDD[VD] = {
        val routingTables = VertexRDD.createRoutingTables(hyperedges,
            this.partitioner.get)
        val vertexPartitions = partitionsRDD.zipPartitions(routingTables,
            preservesPartitioning = true) {
            (partIter, routingTableIter) =>
                val routingTable =
                    if (routingTableIter.hasNext) routingTableIter.next()
                    else RoutingTablePartition.empty
                partIter.map(_.withRoutingTable(routingTable))
        }
        this.withPartitionsRDD(vertexPartitions)
    }

    override protected def getPartitions: Array[Partition] = partitionsRDD
            .partitions

    override protected def getPreferredLocations(s: Partition): Seq[String] =
        partitionsRDD.preferredLocations(s)

    /**
     * Changes the target storage level while preserving all other properties
     * of the
     * VertexRDD. Operations on the returned VertexRDD will preserve this
     * storage level.
     *
     * This does not actually trigger a cache; to do this, call
     * [[org.apache.spark.hyperx.VertexRDD# c a c h e]] on the returned
     * VertexRDD.
     */
    private[hyperx] def withTargetStorageLevel(
        targetStorageLevel: StorageLevel)
    :VertexRDD[VD] = {
        new VertexRDD(this.partitionsRDD, targetStorageLevel)
    }

    /** Generates an RDD of vertex attributes suitable for shipping to the
      * hyperedge partitions. */
    private[hyperx] def shipVertexAttributes(shipSrc: Boolean,shipDst: Boolean)
    : RDD[(PartitionId, VertexAttributeBlock[VD])] = {
        partitionsRDD.mapPartitionsWithIndex((i, part) =>
            part.flatMap{p =>
            val ret = p.shipVertexAttributes(shipSrc,shipDst)
            ret
        }, preservesPartitioning = true)
    }

    private[hyperx] def shipVertexAttributesP(shipSrc: Boolean,shipDst: Boolean,
        t: Array[Accumulator[Int]], tStart: Array[Accumulator[Long]], tCpl: Array[Accumulator[Long]])
    : RDD[(PartitionId, VertexAttributeBlock[VD])] = {
        partitionsRDD.mapPartitionsWithIndex((i, part) =>
            part.flatMap{p =>
                val start = System.currentTimeMillis()
                tStart(i) += start
                val ret = p.shipVertexAttributes(shipSrc,shipDst)
                t(i) += (System.currentTimeMillis() - start).toInt
                tCpl(i) += System.currentTimeMillis()
                ret
            }, preservesPartitioning = true)
    }

    /** Generates an RDD of vertex IDs suitable for shipping to the hyperedge
      *  partitions. */
    private[hyperx] def shipVertexIds(): RDD[(PartitionId, Array[VertexId])] = {
        val activeMsgRDD = partitionsRDD.mapPartitions(
            _.flatMap(_.shipVertexIds()), preservesPartitioning = true)
        activeMsgRDD
    }

    private[hyperx] def shipVertexIdsP(sT: Array[Accumulator[Long]], t: Array[Accumulator[Int]]): RDD[(PartitionId, Array[VertexId])] = {
        partitionsRDD.mapPartitionsWithIndex({(i,p) =>
            val start = System.currentTimeMillis()
            sT(i) += System.currentTimeMillis()
            val ret = p.flatMap(_.shipVertexIds())
            t(i) += (System.currentTimeMillis() - start).toInt
            ret
        }, preservesPartitioning = true)
    }

}

// end of VertexRDD

/**
 * The VertexRDD singleton is used to construct VertexRDDs.
 */
object VertexRDD {

    /**
     * Construct a standalone `VertexRDD`, which is not set up for efficient
     * joins with
     * a [[HyperedgeRDD]]. This is from an RDD of vertex-attribute pairs.
     * Duplicate entries
     * are removed arbitrarily.
     *
     * @todo partitioning strategy
     * @param vertices the vertex-attribute pairs
     * @tparam VD the vertex attribute type
     * @return a new `VertexRDD`
     */
    def apply[VD: ClassTag](vertices: RDD[(VertexId, VD)]): VertexRDD[VD] = {
        val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
            case Some(p) => vertices
            case None => vertices
        }
        val vertexPartitions = vPartitioned.mapPartitions(
            iter => Iterator(ShippableVertexPartition(iter)),
            preservesPartitioning = true)
        new VertexRDD(vertexPartitions)
    }

    /**
     * Construct a `VertexRDD` from an RDD of vertex-attribute pairs.
     * Duplicate vertex attributes
     * are removed arbitrarily. The resulting `VertexRDD` will be joinable
     * with `hyperedges`, and
     * any missing vertices referred to by `hyperedges` will be created with
     * the attribute
     * `defaultVal`.
     * @param vertices the collection of vertex-attribute pairs
     * @param hyperedges the [[HyperedgeRDD]] that these vertices may be
     *                   joined with
     * @param defaultVal the vertex attribute to use when creating missing
     *                   vertices
     * @tparam VD the vertex attribute type
     * @return a new `VertexRDD`
     */
    def apply[VD: ClassTag](
        vertices: RDD[(VertexId, VD)],
        hyperedges: HyperedgeRDD[_, _],
        defaultVal: VD): VertexRDD[VD] = {
        VertexRDD(vertices, hyperedges, defaultVal, (a, b) => b)
    }

    /**
     * Construct a `VertexRDD` from an RDD of vertex-attribute pairs.
     * Duplicate vertex attributes
     * are removed arbitrarily. The resulting `VertexRDD` will be joinable
     * with `hyperedges`, and
     * any missing vertices referred by `hyperedges` will be created with the
     * attribute `defaultVal`.
     *
     * @todo partition strategy
     * @param vertices the collection of vertex-attribute pairs
     * @param hyperedges the [[HyperedgeRDD]] that these vertices may be
     *                   joined with
     * @param defaultVal the vertex attribute to use when creating missing
     *                   vertices
     * @param mergeFunc the commutative, associative duplicate vertex
     *                  attribute merge
     * @tparam VD the vertex attribute type
     * @return a new `VertexRDD`
     */
    def apply[VD: ClassTag](
        vertices: RDD[(VertexId, VD)],
        hyperedges: HyperedgeRDD[_, _],
        defaultVal: VD, mergeFunc: (VD, VD) => VD)
    : VertexRDD[VD] = {
        val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
            case Some(p) => vertices
            case None => vertices.copartitionWithVertices(
                new HashPartitioner(vertices.partitions.size))
        }
        val routingTables = createRoutingTables(hyperedges,
            vPartitioned.partitioner.get)

        val vertexPartitions = vPartitioned.zipPartitions(
            routingTables, preservesPartitioning = true) {
                (vertexIter, routingTableIter) =>
                    val routingTable =
                        if (routingTableIter.hasNext) routingTableIter.next()
                        else RoutingTablePartition.empty
            Iterator(
                ShippableVertexPartition(vertexIter, routingTable, defaultVal))
        }
        new VertexRDD(vertexPartitions)
    }

    def apply[VD: ClassTag](vertices: RDD[(VertexId, VD)],
        hyperedges: HyperedgeRDD[_, _], defaultVal: VD, partitioner: Partitioner,
        storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : VertexRDD[VD] = {
        val vPartitioned = vertices.copartitionWithVertices(partitioner)
        val routingTables = createRoutingTables(hyperedges, partitioner)
        val vertexPartitions = vPartitioned.zipPartitions(routingTables,
            preservesPartitioning = true) {
            (vertexIter, routingTableIter) =>
                val routingTable =
                    if (routingTableIter.hasNext) routingTableIter.next()
                    else RoutingTablePartition.empty
                Iterator(ShippableVertexPartition(vertexIter, routingTable,
                    defaultVal))
        }
        new VertexRDD(vertexPartitions, storageLevel)
    }

    private def createRoutingTables(hyperedges: HyperedgeRDD[_, _],
        vertexPartitioner: Partitioner): RDD[RoutingTablePartition] = {
        val vid2pid = hyperedges.partitionsRDD.mapPartitions(_.flatMap(
            Function.tupled(
//                RoutingTablePartition.hyperedgePartitionToMsgs(_, _))))
                  RoutingTablePartition.flatHyperedgePartitionToMsgs(_, _))))
                .setName("VertexRDD.createRoutingTables - vid2pid (aggregation)")

        val numHyperedgePartitions = hyperedges.partitions.size

        vid2pid.copartitionWithVertices(vertexPartitioner).mapPartitions(
            iter => Iterator(
                RoutingTablePartition.fromMsgs(numHyperedgePartitions, iter)),
            preservesPartitioning = true)

    }

    def fromHyperedges[VD: ClassTag](hyperedges: HyperedgeRDD[_, _],
        numPartitions: Int, defaultVal: VD): VertexRDD[VD] = {
        val routingTables = createRoutingTables(hyperedges,
            new HashPartitioner(numPartitions))
        val vertexPartitions = routingTables.mapPartitions({ routingTableIter =>
            val routingTable =
                if (routingTableIter.hasNext) routingTableIter.next()
                else RoutingTablePartition.empty
            Iterator(ShippableVertexPartition(Iterator.empty,
                routingTable, defaultVal))
        }, preservesPartitioning = true)
        new VertexRDD(vertexPartitions)
    }

}
