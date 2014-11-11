package org.apache.spark.hyperx.impl

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.hyperx._
import org.apache.spark.hyperx.partition.{PartitionStrategy, VertexPartitioner}
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap
import org.apache.spark.hyperx.util.{BytecodeUtils, HyperUtils}
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}
import scala.util.Random

/**
 * An implementation of [[org.apache.spark.hyperx.Hypergraph]] to support
 * computation on hypergraphs.
 *
 * Hypergraphs are represented using two RDDs: `vertices`, which contains vertex
 * attributes and the routing information for shipping vertex attributes to
 * hyperedge partitions, and `replicatedVertexView`, which contains hyperedges
 * and the (read-only) vertex attributes mentioned by each hyperedge.
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */
class HypergraphImpl[VD: ClassTag, ED: ClassTag] protected(
    @transient val vertices: VertexRDD[VD],
    @transient val replicatedVertexView: ReplicatedVertexView[VD, ED]) extends
    Hypergraph[VD, ED] with Serializable with Logging {

    /** Return an RDD that brings hyperedges together with their source and
      * destination vertex sets */
    @transient override lazy val tuples: RDD[HyperedgeTuple[VD, ED]] = {
        replicatedVertexView.upgrade(vertices, includeSrc = true,
            includeDst = true)
        replicatedVertexView.hyperedges.partitionsRDD.mapPartitions(_.flatMap {
            case (pid, part) => part.tupleIterator()
        })
    }
    @transient override val hyperedges: HyperedgeRDD[ED, VD] =
        replicatedVertexView.hyperedges

    override def persist(newLevel: StorageLevel): Hypergraph[VD, ED] = {
        vertices.persist(newLevel)
        replicatedVertexView.hyperedges.persist(newLevel)
        this
    }

    override def cache(): HypergraphImpl[VD, ED] = {
        vertices.cache()
        replicatedVertexView.hyperedges.cache()
        this
    }

    override def unpersistVertices(blocking: Boolean = true)
    : Hypergraph[VD,ED] = {
        vertices.unpersist(blocking)
        this
    }

    override def partitionBy(strategy: PartitionStrategy)
    : Hypergraph[VD, ED] = {
        partitionBy(strategy, hyperedges.partitions.size)
    }

    /**
     * Partition the hyperedges and vertices using the supplied strategy.
     *
     */
    override def partitionBy(strategy: PartitionStrategy,
                             numPartitions: Int): Hypergraph[VD, ED] = {
        null.asInstanceOf[Hypergraph[VD, ED]]
    }


    override def reverse: Hypergraph[VD, ED] = {
        new HypergraphImpl(vertices.reverseRoutingTables(),
            replicatedVertexView.reverse())
    }

    override def mapVertices[VD2: ClassTag](map: (VertexId,VD) => VD2)
    : Hypergraph[VD2, ED] = {
        val vdTag = classTag[VD]
        val vd2Tag = classTag[VD2]
        if (vdTag == vd2Tag) {
            vertices.cache()
            val newVerts = vertices.mapVertexPartitions{p =>
                p.map(map)}.cache()
            val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]]
                    .diff(newVerts)
            val newReplicatedVertexView = replicatedVertexView
                    .asInstanceOf[ReplicatedVertexView[VD2, ED]]
                    .updateVertices(changedVerts)
            new HypergraphImpl(newVerts, newReplicatedVertexView)
        }
        else {
            HypergraphImpl(vertices.mapVertexPartitions(_.map(map)),
                replicatedVertexView.hyperedges)
        }
    }

//    override def mapHyperedges[ED2: ClassTag](
//        f: (PartitionId,Iterator[Hyperedge[ED]]) => Iterator[ED2])
//    : Hypergraph[VD, ED2] = {
//        val newHyperedges = replicatedVertexView.hyperedges
//                .mapHyperedgePartitions((pid, part) => part.map(f(pid,
//            part.iterator)))
//        new HypergraphImpl(vertices, replicatedVertexView.withHyperedges
//                (newHyperedges))
//    }

    override def mapHyperedges[ED2: ClassTag](
        f: (PartitionId, Iterator[Hyperedge[ED]]) => Iterator[(HyperedgeId, ED2)])
    : Hypergraph[VD, ED2] = {
        val newHyperedges = replicatedVertexView.hyperedges
            .mapHyperedgePartitions((pid, part) =>
            part.map(f(pid, part.iterator)))
        new HypergraphImpl(vertices,
            replicatedVertexView.withHyperedges(newHyperedges))
    }

    override def assignHyperedgeId(): Hypergraph[VD, HyperedgeId] = {
        val partitionSizes = hyperedges.partitionsRDD.map(p => (p._1, p._2.size)).collect()
        val pids = partitionSizes.map(_._1).toSet
        val offsets = partitionSizes.sortBy(_._1)
            .map(p => (p._1, (0 until p._1).filter(pids).map(i => partitionSizes(i)._2).sum)).toMap
        mapHyperedges((pid, iter) => iter.map(h => (h.id, offsets(pid) + h.id)))
    }

    override def getHyperedgeIdWeightPair: RDD[(HyperedgeId, Double)] = {
        val partitionSizes = hyperedges.partitionsRDD.map(p => (p._1, p._2.size)).collect()
        val pids = partitionSizes.map(_._1).toSet
        val offsets = partitionSizes.sortBy(_._1)
            .map(p => (p._1, (0 until p._1).filter(pids).map(i => partitionSizes(i)._2).sum)).toMap
        hyperedges.partitionsRDD.flatMap{p =>
            p._2.iterator.map(h => (offsets(p._1) + h.id, h.attr match{
                case d: Double => d
                case _ => 1.0
            }))
        }
    }

    override def toDoubleWeight: Hypergraph[VD, Double] = {
        mapHyperedges((pid, iter) => iter.map(h => (h.id, h.attr match {
            case d: Double => d
            case _ => 1.0
        })))
    }

//    override def mapTuples[ED2: ClassTag](
//        f: (PartitionId,Iterator[HyperedgeTuple[VD, ED]]) => Iterator[ED2])
//    : Hypergraph[VD, ED2] = {
//        vertices.cache()
//        val mapUsesSrcAttr = accessesVertexAttr(f, "srcAttr")
//        val mapUsesDstAttr = accessesVertexAttr(f, "dstAttr")
//        replicatedVertexView.upgrade(vertices, mapUsesSrcAttr, mapUsesDstAttr)
//        val newHyperedges = replicatedVertexView.hyperedges
//                .mapHyperedgePartitions { (pid, part) =>
//            part.map(f(pid, part.tupleIterator(mapUsesSrcAttr, mapUsesDstAttr)))
//        }
//        new HypergraphImpl(vertices, replicatedVertexView.withHyperedges
//                (newHyperedges))
//    }
    override def mapTuples[ED2: ClassTag](
        f: (PartitionId, Iterator[HyperedgeTuple[VD, ED]]) =>
                Iterator[(HyperedgeId, ED2)])
    : Hypergraph[VD, ED2] = {
        val mapUsesSrcAttr = accessesVertexAttr(f, "srcAttr")
        val mapUsesDstAttr = accessesVertexAttr(f, "dstAttr")

        replicatedVertexView.upgrade(vertices, mapUsesSrcAttr, mapUsesDstAttr)
        val newHyperedges = replicatedVertexView.hyperedges
            .mapHyperedgePartitions{(pid, part) =>
                part.map{f(pid, part.tupleIterator(mapUsesSrcAttr, mapUsesDstAttr))}
            }
        new HypergraphImpl(vertices, replicatedVertexView.withHyperedges(newHyperedges))
    }

    override def subgraph(
        hpred: HyperedgeTuple[VD,ED] => Boolean = x => true,
        vspred: HyperAttr[VD] => Boolean = y => true,
        vpred: (VertexId, VD) => Boolean = (a,b) =>true)
    : Hypergraph[VD, ED] = {
        vertices.cache()
        val newVerts = vertices.mapVertexPartitions(_.filter(vpred))
        replicatedVertexView.upgrade(vertices, includeSrc = true,
            includeDst = true)
        val newHyperedges = replicatedVertexView.hyperedges.filter(hpred,
            vspred)
        new HypergraphImpl(newVerts, replicatedVertexView.withHyperedges
                (newHyperedges))
    }

    override def mask[VD2: ClassTag, ED2: ClassTag](
        other: Hypergraph[VD2,ED2]): Hypergraph[VD, ED] = {
        val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v}
        val newHyperedges = replicatedVertexView.hyperedges.innerJoin(other
                .hyperedges) { (src, dst, v, w) => v}
        new HypergraphImpl(newVerts, replicatedVertexView.withHyperedges
                (newHyperedges))
    }

    override def groupHyperedges(merge: (ED, ED) => ED): Hypergraph[VD, ED] = {
        val newHyperedges = replicatedVertexView.hyperedges
                .mapHyperedgePartitions(
                    (pid, part) => part.groupHyperedges(merge)
                )
        new HypergraphImpl(vertices, replicatedVertexView.withHyperedges
                (newHyperedges))
    }

    override def mapReduceTuples[A: ClassTag](
        mapFunc: HyperedgeTuple[VD,ED] => Iterator[(VertexId, A)],
        reduceFunc: (A, A) => A,
        activeSetOpt: Option[(VertexRDD[_], HyperedgeDirection)] = None)
    : VertexRDD[A] = {
        vertices.cache()

        // Check whether the vertex attributes need to be shipped
        val mapUsesSrcAttr = accessesVertexAttr(mapFunc, "srcAttr")
        val mapUsesDstAttr = accessesVertexAttr(mapFunc, "dstAttr")

        // ship attributes accordingly
        replicatedVertexView.upgrade(vertices, mapUsesSrcAttr, mapUsesDstAttr)

        val view = activeSetOpt match {
            case Some((activeSet, _)) =>
                replicatedVertexView.withActiveSet(activeSet)
            case None =>
                replicatedVertexView
        }
        val activeDirectionOpt = activeSetOpt.map(_._2)

        val preAgg = view.hyperedges.partitionsRDD.mapPartitions({
            p => p.flatMap {
                // choose whether to use index to iterate the tuples
                case (pid, hyperedgePartition) =>
                    val activeFraction =
                        hyperedgePartition.numActives.getOrElse(0) /
                                hyperedgePartition.sourceSize.toFloat

                    val hyperedgeIter = activeDirectionOpt match {
                        case Some(HyperedgeDirection.Both) =>
                            if (activeFraction < 0.8) {
                                hyperedgePartition.indexIterator(srcVertexId =>
                                    hyperedgePartition.isActive(srcVertexId))
                                        .filter(h => hyperedgePartition
                                            .isActive(h.dstIds))
                            } else {
                                hyperedgePartition.iterator.filter(h =>
                                    hyperedgePartition.isActive(h.srcIds) &&
                                        hyperedgePartition.isActive(h.dstIds))
                            }
                        case Some(HyperedgeDirection.Either) =>
                            // Scan all hyperedges and then do the filter.
                            hyperedgePartition.iterator.filter(h =>
                                hyperedgePartition.isActive(h.srcIds) ||
                                        hyperedgePartition.isActive(h.dstIds))
                        case Some(HyperedgeDirection.Out) =>
                            if (activeFraction < 0.8) {
                                hyperedgePartition.indexIterator(srcVertexId =>
                                    hyperedgePartition.isActive(srcVertexId))
                            } else {
                                hyperedgePartition.iterator.filter(h =>
                                    hyperedgePartition.isActive(h.srcIds))
                            }
                        case Some(HyperedgeDirection.In) =>
                            hyperedgePartition.iterator.filter(h =>
                                hyperedgePartition.isActive(h.srcIds))
                        case _ => hyperedgePartition.iterator
                    }

                    // generate hyperedge tuple iterators
                    // todo: to balance the hyperedge processing
                    val mapOutputs = hyperedgePartition.upgradeIterator(
                        hyperedgeIter, mapUsesSrcAttr, mapUsesDstAttr)
                            .flatMap(mapFunc)
                    hyperedgePartition.vertices.aggregateUsingIndex(mapOutputs,
                        reduceFunc).iterator
                }

        }, preservesPartitioning = true).setName("HypergraphImpl.mapReduceTuples - preAgg")


        vertices.aggregateUsingIndex(preAgg, reduceFunc)
    }

  /**
   *  The same method with the capability to track the time spent on different
   *  phases during the execution.
   *
   *  Map phase => mT, combine phase => cT, map and combine phase => mcT,
   *  reduce phase => rT
   */
    override def mapReduceTuplesP[A: ClassTag](sc: SparkContext,
        msT: Array[Accumulator[Int]], mdT: Array[Accumulator[Int]],
        msdT: Array[Accumulator[Int]], mddT: Array[Accumulator[Int]],
        cT: Array[Accumulator[Int]],
        mcT: Array[Accumulator[Int]], rT: Array[Accumulator[Int]],
        sT: Array[Accumulator[Int]], zT: Array[Accumulator[Int]],
        mStart: Array[Accumulator[Long]], cStart: Array[Accumulator[Long]],
        rStart: Array[Accumulator[Long]],
        sStart: Array[Accumulator[Long]], zStart: Array[Accumulator[Long]],
        mCpl: Array[Accumulator[Long]], cCpl: Array[Accumulator[Long]],
        rCpl: Array[Accumulator[Long]], rCount: Array[Accumulator[Int]],
        cCount: Array[Accumulator[Int]],
        sCpl: Array[Accumulator[Long]], zCpl: Array[Accumulator[Long]],
        mrStart: Long,
        mapFunc: (HyperedgeTuple[VD,ED], Accumulator[Int], Accumulator[Int],
            Accumulator[Int], Accumulator[Int]) => Iterator[(VertexId, A)],
        reduceFunc: (A, A) => A,
        activeSetOpt: Option[(VertexRDD[_], HyperedgeDirection)] = None)
    : VertexRDD[A] = {

        // Check whether the vertex attributes need to be shipped
        val mapUsesSrcAttr = accessesVertexAttr(mapFunc, "srcAttr")
        val mapUsesDstAttr = accessesVertexAttr(mapFunc, "dstAttr")

        // ship attributes accordingly
        replicatedVertexView.upgradeP(vertices,
            mapUsesSrcAttr, mapUsesDstAttr, sT, zT, sStart,
            zStart, sCpl, zCpl, mrStart)

        val view = activeSetOpt match {
            case Some((activeSet, _)) =>
                replicatedVertexView.withActiveSetP(activeSet,
                    sStart, sCpl, sT, zT)
            case None =>
                replicatedVertexView
        }
        val activeDirectionOpt = activeSetOpt.map(_._2)

        val preAgg = view.hyperedges.partitionsRDD.mapPartitions({
            p => p.flatMap {
                case (pid, hyperedgePartition) =>
                    val start = System.currentTimeMillis()
//                    zStart(pid) += start
                    val activeFraction =
                        hyperedgePartition.numActives.getOrElse(0) /
                                hyperedgePartition.sourceSize.toFloat
                    val hyperedgeIter = activeDirectionOpt match {
                        case Some(HyperedgeDirection.Both) =>
                            if (activeFraction < 0.8) {
                                hyperedgePartition.indexIterator(srcVertexId =>
                                    hyperedgePartition.isActive(srcVertexId))
                                        .filter(h => hyperedgePartition.isActive
                                        (h.dstIds))
                            } else {
                                hyperedgePartition.iterator.filter(h =>
                                    hyperedgePartition.isActive(h.srcIds) &&
                                         hyperedgePartition.isActive(h.dstIds))
                            }
                        case Some(HyperedgeDirection.Either) =>
                            // Scan all hyperedges and then do the filter.
                            hyperedgePartition.iterator.filter(h =>
                                hyperedgePartition.isActive(h.srcIds) ||
                                        hyperedgePartition.isActive(h.dstIds))
                        case Some(HyperedgeDirection.Out) =>
                            if (activeFraction < 0.8) {
                                hyperedgePartition.indexIterator(srcVertexId =>
                                    hyperedgePartition.isActive(srcVertexId))
                            } else {
                                hyperedgePartition.iterator.filter(h =>
                                    hyperedgePartition.isActive(h.srcIds))
                            }
                        case Some(HyperedgeDirection.In) =>
                            hyperedgePartition.iterator.filter(h =>
                                hyperedgePartition.isActive(h.srcIds))
                        case _ => hyperedgePartition.iterator
                    }
//                    zCpl(pid) += System.currentTimeMillis()
                    mStart(pid) += System.currentTimeMillis()
                    // generate hyperedge tuple iterators
                    val mapIterator = hyperedgePartition.upgradeIterator(
                        hyperedgeIter, mapUsesSrcAttr,mapUsesDstAttr)
                    val mapOutputs = mapIterator.flatMap(h =>
                        mapFunc(h, msT(pid), mdT(pid), msdT(pid), mddT(pid)))
                        .toIndexedSeq
                    mCpl(pid) += System.currentTimeMillis()
                    val ret = hyperedgePartition.vertices
                            .aggregateUsingIndexCP(mapOutputs.iterator,
                            reduceFunc, cT(pid), cStart(pid), cCpl(pid),
                            cCount(pid)).iterator
                    mcT(pid) += (System.currentTimeMillis() - start).toInt
                    ret
                }

        }, preservesPartitioning = true)
        .setName("HypergraphImpl.mapReduceTuples - preAgg")

        vertices.aggregateUsingIndexP(preAgg, reduceFunc, rT,
            rStart, rCpl, rCount)
    }

    private def accessesVertexAttr(closure: AnyRef, attrName: String)
    : Boolean = {
        try {
            BytecodeUtils.invokedMethod(closure,
                classOf[HyperedgeTuple[VD, ED]], attrName)
        } catch {
            case _: ClassNotFoundException => true
        }
    }

    override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
    (other: RDD[(VertexId, U)])
    (updateF: (VertexId, VD, Option[U]) => VD2)
    : Hypergraph[VD2, ED] = {
        val vdTag = classTag[VD]
        val vd2Tag = classTag[VD2]
        if (vdTag == vd2Tag) {
            vertices.cache()

            // update the vertex attribute values
            val newVerts = vertices.leftJoin(other)(updateF).cache()

            // update the replicas, the fewer replicas, the less cost here
            val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(
                newVerts)
            val newReplicatedVertexView = replicatedVertexView
                    .asInstanceOf[ReplicatedVertexView[VD2, ED]]
                    .updateVertices(changedVerts)

            new HypergraphImpl(newVerts, newReplicatedVertexView)
        } else {
            val newVerts = vertices.leftJoin(other)(updateF)
            HypergraphImpl(newVerts, replicatedVertexView.hyperedges)
        }
    }

    protected def this() = this(null, null)
}

object HypergraphImpl extends Logging {

    /** Create a hypergraph from hyperedges, settings referenced vertices to
    `defaultVertexAttr` **/
    def apply[VD: ClassTag, ED: ClassTag](hyperedges: RDD[Hyperedge[ED]],
        defaultVertexAttr: VD, hyperedgeStorageLevel: StorageLevel,
        vertexStorageLevel:StorageLevel) : HypergraphImpl[VD, ED] = {
        fromHyperedgeRDD(HyperedgeRDD.fromHyperedges(hyperedges),
            defaultVertexAttr,
            hyperedgeStorageLevel, vertexStorageLevel)
    }

    /**
     * Create a hypergraph from a HyperedgeRDD with the correct vertex type,
     * setting missing vertices to `defaultVertexAttr`. The vertices will have
     * the same number of partitions as the HyperedgeRDD.
     */
    private def fromHyperedgeRDD[VD: ClassTag, ED: ClassTag](
        hyperedges: HyperedgeRDD[ED, VD], defaultVertexAttr: VD,
        hyperedgeStorageLevel: StorageLevel, vertexStorageLevel: StorageLevel)
    : HypergraphImpl[VD, ED] = {
        val hyperedgesCached = hyperedges.withTargetStorageLevel(
            hyperedgeStorageLevel).cache()
        val vertices = VertexRDD.fromHyperedges(hyperedgesCached,
            hyperedgesCached.partitions.size, defaultVertexAttr)
                .withTargetStorageLevel(vertexStorageLevel)
        fromExistingRDDs(vertices, hyperedgesCached)
    }

    /**
     * Create a hypergraph from a VertexRDD and a HyperedgeRDD with the same
     * replicated vertex type as the vertices. The VertexRDD must already be set
     * up for efficient joins with the HyperedgeRDD by calling
     * `VertexRDD.withHyperedges` or an appropriate VertexRDD constructor.
     */
    def fromExistingRDDs[VD: ClassTag, ED: ClassTag]( vertices: VertexRDD[VD],
        hyperedges: HyperedgeRDD[ED, VD]): HypergraphImpl[VD, ED] = {
        new HypergraphImpl(vertices, new ReplicatedVertexView(hyperedges))
    }

    /** Create a hypergraph from HyperedgePartitions,
      * setting referenced vertices to `defaultVertexAttr`. */
    def fromHyperedgePartitions[VD: ClassTag, ED: ClassTag](
//        hyperedgePartitions: RDD[(PartitionId, HyperedgePartition[ED, VD])],
        hyperedgePartitions: RDD[(PartitionId, FlatHyperedgePartition[ED, VD])],
        defaultVertexAttr: VD,
        hyperedgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : HypergraphImpl[VD, ED] = {
        fromHyperedgeRDD(HyperedgeRDD.fromHyperedgePartitions
                (hyperedgePartitions), defaultVertexAttr,
            hyperedgeStorageLevel, vertexStorageLevel)
    }

    def fromHyperedgeList[VD: ClassTag, ED: ClassTag](input: RDD[String],
        numParts: Int, strategy: PartitionStrategy,
        vertexLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        hyperedgeLevel: StorageLevel = StorageLevel.MEMORY_ONLY) = {

        val rdds = strategy.partition(numParts, input)
        val localVertices = rdds._1.map{v =>
            Tuple2(v._1, (v._2, null.asInstanceOf[VD]))
        }.partitionBy(new HashPartitioner(numParts))
                .flatMap(p => Iterator(p._2))
        val vertexPartitioner: Partitioner = strategy.getPartitioner

//        val hyperedges: RDD[(Int, HyperedgePartition[ED, VD])] =
        val hyperedges: RDD[(Int, FlatHyperedgePartition[ED, VD])] =
            rdds._2.partitionBy(new HashPartitioner(numParts))
                    .mapPartitions({p =>
//                val builder = new HyperedgePartitionBuilder[ED, VD]()
                val builder = new FlatHyperedgePartitionBuilder[ED, VD]()
                var pid = 0
                p.zipWithIndex.foreach{h =>
                    val pair = HyperUtils.hyperedgeFromHString(h._1._2)
                    pid = h._1._1
                    builder.add(pair._1, pair._2, h._2, null.asInstanceOf[ED])
                }
//                Iterator((pid, builder.toHyperedgePartition))
                Iterator((pid, builder.toFlatHyperedgePartition))
            }, preservesPartitioning = true)
        val hyperedgesRDD =
            HyperedgeRDD.fromHyperedgePartitions(hyperedges, hyperedgeLevel)

        val vertexRDD = VertexRDD[VD](localVertices, hyperedgesRDD,
            null.asInstanceOf[VD], vertexPartitioner, vertexLevel)
        new HypergraphImpl(vertexRDD, new ReplicatedVertexView(hyperedgesRDD))
    }

    def fromPartitions[VD: ClassTag, ED: ClassTag](
//        hyperedges: RDD[(Int, HyperedgePartition[ED, VD])],
        hyperedges: RDD[(Int, FlatHyperedgePartition[ED, VD])],
        vertices: RDD[ShippableVertexPartition[VD]],
        hyperedgeLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        vertexLevel: StorageLevel = StorageLevel.MEMORY_ONLY) = {
        val hyperedgesRDD = new HyperedgeRDD[ED,VD](hyperedges, hyperedgeLevel)
        val vertexRDD = new VertexRDD[VD](vertices, vertexLevel)
        new HypergraphImpl(vertexRDD, new ReplicatedVertexView(hyperedgesRDD))
    }

    def fromPartitionedHyperedgeList[VD: ClassTag, ED: ClassTag](
        input: RDD[String], numParts: Int,
        vertexLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        hyperedgeLevel: StorageLevel = StorageLevel.MEMORY_ONLY) = {
        // randomized vertex partitioning
        val vertices = input.flatMap(line =>
            HyperUtils.iteratorFromPartitionedString(line)).distinct()
                .map(v => (v, Random.nextInt(numParts)))
        val collectedVertices = vertices.collectAsMap()
        val vertexMap = new HyperXOpenHashMap[VertexId, PartitionId]()
        collectedVertices.foreach(e => vertexMap.update(e._1, e._2))
        val partitioner: Partitioner =
            new VertexPartitioner(numParts, vertexMap)
        val localVertices = vertices.map(v =>
            Tuple2(v._2, (v._1, null.asInstanceOf[VD])))
                .partitionBy(new HashPartitioner(numParts))
                .flatMap(p => Iterator(p._2))
//        val hyperedges: RDD[(Int, HyperedgePartition[ED, VD])] =
        val hyperedges: RDD[(Int, FlatHyperedgePartition[ED, VD])] =
            input.map(HyperUtils.pairFromPartitionedString)
                    .partitionBy(new HashPartitioner(numParts))
                    .mapPartitions({p =>
//                val builder = new HyperedgePartitionBuilder[ED, VD]()
                val builder = new FlatHyperedgePartitionBuilder[ED, VD]()
                var pid = 0
                p.zipWithIndex.foreach{h =>
                    val pair = HyperUtils.hyperedgeFromHString(h._1._2)
                    pid = h._1._1
                    builder.add(pair._1, pair._2, h._2, null.asInstanceOf[ED])
                }
                Iterator((pid, builder.toFlatHyperedgePartition))
            }, preservesPartitioning = true)
        val hyperedgesRDD =
            HyperedgeRDD.fromHyperedgePartitions(hyperedges, hyperedgeLevel)

        val vertexRDD = VertexRDD[VD](localVertices, hyperedgesRDD,
            null.asInstanceOf[VD], partitioner, vertexLevel)
        new HypergraphImpl(vertexRDD, new ReplicatedVertexView(hyperedgesRDD))
    }

    /** Create a hypergraph from vertices and hyperedges,
      * setting missing vertices to `defaultVertexAttr`. */
    def apply[VD: ClassTag, ED: ClassTag](vertices: RDD[(VertexId, VD)],
        hyperedges: RDD[Hyperedge[ED]], defaultVertexAttr: VD,
        hyperedgeStorageLevel: StorageLevel, vertexStorageLevel: StorageLevel)
    : HypergraphImpl[VD, ED] = {
        val hyperedgeRDD = HyperedgeRDD.fromHyperedges(hyperedges)(
            classTag[ED], classTag[VD])
                .withTargetStorageLevel(hyperedgeStorageLevel).cache()
        val vertexRDD = VertexRDD(vertices, hyperedgeRDD, defaultVertexAttr)
                .withTargetStorageLevel(vertexStorageLevel).cache()
        HypergraphImpl(vertexRDD, hyperedgeRDD)
    }

    /**
     * Create a hypergraph from a VertexRDD and a HyperedgeRDD with arbitrary
     * replicated vertices. The VertexRDD must already be set up for efficient
     * joins with the HyperedgeRDD by calling `VertexRDD.withHyperedges` or an
     * appropriate VertexRDD constructor.
     */
    def apply[VD: ClassTag, ED: ClassTag](vertices: VertexRDD[VD],
                                          hyperedges: HyperedgeRDD[ED, _])
    : HypergraphImpl[VD, ED] = {
        val newHyperedges = hyperedges.mapHyperedgePartitions(
            (pid, part) => part.withVertices(part.vertices.map(
                (vid, attr) => null.asInstanceOf[VD])))
        HypergraphImpl.fromExistingRDDs(vertices, newHyperedges)
    }

}
