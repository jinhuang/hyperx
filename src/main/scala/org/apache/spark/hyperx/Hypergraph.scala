package org.apache.spark.hyperx

import org.apache.spark.{SparkContext, Accumulator}
import org.apache.spark.hyperx.impl.HypergraphImpl
import org.apache.spark.hyperx.partition.PartitionStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * The Hypergraph abstractly represents a hypergraph with arbitrary objects
 * associated with vertices and hyperedges. The hypergraph provides basic
 * operations to access and manipulate the data associated with vertices and
 * hyperedges as well as the underlying structure. Like Spark RDDs, the
 * hypergraph is a functional data-structure in which mutating operations return
 * new hypergraphs
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the hyperedge attribute type
 *
 *            Forked from GraphX, modified by Jin Huang
 */
abstract class Hypergraph[VD: ClassTag, ED: ClassTag] protected() extends
Serializable {

    /**
     * An RDD containing the vertices and their associated attributes
     *
     * @note vertex ids are unique.
     * @return an RDD containing the vertices in this hypergraph
     */
    @transient val vertices: VertexRDD[VD]

    /**
     * An RDD containing the hyperedges and their associated attributes. The
     * entries in the
     * RDD contain just the source id and destination id along with the
     * hyperedge data.
     *
     * @return an RDD containing the hyperedges in this graph
     */
    @transient val hyperedges: HyperedgeRDD[ED, VD]

    /**
     * An RDD containing the hyperedge tuples, which are hyperedges along
     * with the vertex
     * attributes associated with the adjacent vertices. The caller should
     * use [[hyperedges]]
     * if the vertex data are not needed, i.e., if only the hyperedge data
     * and adjacent vertex
     * ids are needed.
     *
     * @return an RDD containing hyperedge tuples
     */
    @transient val tuples: RDD[HyperedgeTuple[VD, ED]]

    /**
     * Cache the vertices and hyperedges associated with this hypergraph at
     * the specific storage
     * level, ignoring any target storage levels previously set.
     *
     * @param newLevel the level at which to cache the hypergraph.
     * @return A reference to this hypergraph for convenience
     */
    def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY):
    Hypergraph[VD, ED]

    /**
     * Cache the vertices and hyperedges associated with this hypergraph at
     * the previously specified
     * target storage levels, which default to `MEMORY_ONLY`. This is used to
     * pin a hypergraph in
     * memory enabling multiple queries to reuse the same construction process.
     */
    def cache(): Hypergraph[VD, ED]

    /**
     * Uncache only the vertices of this hypergraph, leaving the hyperedges
     * alone. This is useful
     * in iterative algoirthms that modify the vertex attributes but reuse
     * the hyperedges. This
     * method can be used to uncache the vertex attributes of previous
     * iterations once they are
     * no longer needed, improving the GC performance.
     */
    def unpersistVertices(blocking: Boolean = true): Hypergraph[VD, ED]

    /**
     * Repartition the hyperedges and the vertices in the hypergraph accoing
     * to `hPartitionStrategty`
     * and `vPartitionStrategy`
     * @param partitionStrategy the partition strategy for hyperedges and
     *                          vertices
     */
    def partitionBy(partitionStrategy: PartitionStrategy): Hypergraph[VD, ED]

    //  def partitionBy(partitionStrategy: PartitionStrategy,
    // sc: SparkContext): Hypergraph[VD, ED]

    /**
     * Repartition the hyperedges and the vertices in the hypergraph
     * according to `hPartitionStrategy`
     * and `vPartitionStrategy`
     * @param partitionStrategy the partitioning strategy of hyperedges and
     *                          vertices
     * @param numPartitions the number of partitions in the new hypergraph
     */
    def partitionBy(partitionStrategy: PartitionStrategy,
                    numPartitions: Int): Hypergraph[VD, ED]

    /**
     * Transform each vertex attribute in the hypergraph using the map function
     * @param map the function from a vertex object to a new vertex attribute
     *            value
     * @tparam VD2 the new vertex attribute type
     * @return the new hypergraph
     */
    def mapVertices[VD2: ClassTag](map: (VertexId,
            VD) => VD2): Hypergraph[VD2, ED]

    /**
     * Transform each hyperedge attribute in the hypergraph using the map
     * function. The
     * map function is not passed the vertex value for the vertices adjacent
     * to the edge.
     * If vertex values are desired, use `mapTuples`
     * @param map the function from a hyperedge object to a new hyperedge
     *            attribute value
     * @tparam ED2 the new edge attribute type
     */
    def mapHyperedges[ED2: ClassTag](map: Hyperedge[ED] => (HyperedgeId, ED2)):
    Hypergraph[VD, ED2] = {
        mapHyperedges((pid, iter) => iter.map(map))
    }

    def mapHyperedges[ED2: ClassTag](
        f: (PartitionId, Iterator[Hyperedge[ED]]) => Iterator[(HyperedgeId, ED2)]
    ): Hypergraph[VD, ED2]

    def assignHyperedgeId(): Hypergraph[VD, HyperedgeId]

    def toDoubleWeight: Hypergraph[VD, Double]

    def getHyperedgeIdWeightPair: RDD[(HyperedgeId, Double)]

    /**
     * Transforms each hyperedge attributes using the map function,
     * passing it the adjacent
     * vertex attributes as well. If adjacent vertex values are not required,
     * consider using
     * `mapHyperedges` instead.
     * @param map the function from a hyperedge object to the new hyperedge
     *            attribute value
     * @tparam ED2 the new hyperedge attribute type
     */
    def mapTuples[ED2: ClassTag](map: HyperedgeTuple[VD,
            ED] => (HyperedgeId, ED2)): Hypergraph[VD, ED2] = {
        mapTuples((pid, iter) => iter.map(map))
    }

    /**
     * Transform each hyperedge attributes a partition at a time using the
     * map function, passing
     * it the adjacent vertex attributes as well. The map function is given
     * an iterator over hyperedge
     * tuples within a logical partition and should yield a new iterator over
     * the new values of
     * each hyperedge in the order in which they are provided. If adjacent
     * vertex values are not
     * required, consider using `mapHyperedges` insetad.
     * @param map the map function from a partition id and iterator to an
     *            iterator over the
     *            new hyperedge attribute values
     * @tparam ED2 the new hyperedge attribute type
     */
//    def mapTuples[ED2: ClassTag](map: (PartitionId,
//            Iterator[HyperedgeTuple[VD, ED]]) => Iterator[ED2])
//    : Hypergraph[VD, ED2]
    def mapTuples[ED2: ClassTag](map: (PartitionId, Iterator[HyperedgeTuple[VD, ED]]) => Iterator[(HyperedgeId, ED2)])
    : Hypergraph[VD, ED2]

    /**
     * Reverse all hyperedges in the hypergraph.
     */
    def reverse: Hypergraph[VD, ED]

    /**
     * Restrict the hypergraph to only the vertices and hyperedges satisfying
     * the predicates.
     * The resulting subhypergraph satisifies
     * {{{
     * V' = {v: for all v in V where vpred(v)}
     * H' = {h: for all h in H where hpred(h)}
     * }}}
     * @param hpred the hyperedge predicate
     * @param vpred the vertex predicate
     * @return a hypergraph with vertices and hyperedges satisfying the
     *         predicates
     */
    def subgraph(
                        hpred: HyperedgeTuple[VD, ED] => Boolean = x => true,
                        vspred: HyperAttr[VD] => Boolean,
                        vpred: (VertexId, VD) => Boolean = (v, d) => true)
    : Hypergraph[VD, ED]

    /**
     * Restrict the hypergraph to only the vertices and hyperedges that are
     * also in `other`,
     * but keep the attributes from this hypergraph
     * @param other the hypergraph to project this hypergraph to
     * @return a hypergraph with vertices and hyperedges that exist in both
     *         the current
     *         hypergraph and `other` with vertex and hyperedge attributes
     *         from the current
     *         hypergraph
     */
    def mask[VD2: ClassTag, ED2: ClassTag](other: Hypergraph[VD2,
            ED2]): Hypergraph[VD, ED]

    /**
     * Merge multiple hyperedges between the same two vertex sets into a
     * single hyperedge.
     * For correct result, the hypergraph must have been partitioned using
     * [[partitionBy]]
     * @param merge the user-supplied commutative associative function to
     *              merge hyperedge
     *              attributes for duplicate hyperedges
     * @return the resulting hypergraph with a single hyperedge for each
     *         vertex sets pair
     */
    def groupHyperedges(merge: (ED, ED) => ED): Hypergraph[VD, ED]

    /**
     * Aggregates values from the neighboring hyperedges and vertices of each
     * vertex. The
     * user supplied `mapFunc` function is invoked on each hyperedge on the
     * graph, generating
     * 0 or more "messages" to be "sent" to either vertex in that hyperedge.
     * The `reduceFunc`
     * is then used to combine the output of the map phase destined to each
     * vertex
     *
     * @param mapFunc the user supplied map function which returns 0 or more
     *                messages to
     *                neighboring vertices
     * @param reduceFunc the user supplied reduce function which should be
     *                   commutative and
     *                   associative and is used to combine the output of the
     *                   map phase
     * @param activeSetOpt optionally, a set of "active" vertices and a
     *                     direction of hyperedge
     *                     to consider when running `mapFunc`. If the
     *                     direction is `In`,
     *                     `mapFunc` will only be run on hyperedges with
     *                     destination in the
     *                     active set. If the direction is `Out`,
     *                     `mapFunc` will only be run
     *                     on hyperedges originating from vertices in the
     *                     active set. If the
     *                     direction is `Either`, `mapFunc` will be run on
     *                     hyperedges with
     *                     *either* vertices in the active set. If the
     *                     direction is `Both`,
     *                     `mapFunc` will be run on hyperedges with *both*
     *                     vertices in the
     *                     active set. The active set must have the same
     *                     index as the
     *                     hypergraph's vertices.
     * @tparam A the type of "messages" to be sent to each vertex
     */
    def mapReduceTuples[A: ClassTag](
        mapFunc: HyperedgeTuple[VD,ED] => Iterator[(VertexId, A)],
        reduceFunc: (A, A) => A,
        activeSetOpt: Option[(VertexRDD[_],HyperedgeDirection)] = None)
    : VertexRDD[A]

    def mapReduceTuplesP[A: ClassTag](sc: SparkContext,
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
        mapFunc: (HyperedgeTuple[VD,ED], Accumulator[Int], Accumulator[Int], Accumulator[Int], Accumulator[Int]) => Iterator[(VertexId, A)],
        reduceFunc: (A, A) => A,
        activeSetOpt: Option[(VertexRDD[_],HyperedgeDirection)] = None)
    : VertexRDD[A]

    /**
     * Joins the vertices with entries in the `table` RDD and merges the results
     * using `mapFunc`. The input table should contain at most one entry for
     * each vertex. If no entry in `other` is provided for a particular vertex
     * in the graph, the map function receives `None`.
     * @param other the table to join with the vertices in the hypergraph.
     *              The table should contain at most one entry for each vertex.
     * @param mapFunc the function used to compute the new vertex values. The
     *                map function is invoked for all vertices, even those that
     *                do not have a corresponding entry in the table.
     * @tparam U the type of entry in the table of updates
     * @tparam VD2 the new vertex value type
     */
    def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
        (mapFunc: (VertexId, VD, Option[U]) => VD2)
    : Hypergraph[VD2, ED]

    /**
     * The associated [[HypergraphOps]] objects.
     *
     * Save a copy of the HypergraphOps object so there is always one unique
     * HypergraphOps object
     * for a given hypergraph object, and thus the lazy vals in HypergraphOps
     * would work as intended.
     */
    val ops = new HypergraphOps(this)

}

/**
 * The Hypergraph object contains a collection of routines used to construct
 * hypergraphs from RDDs.
 */
object Hypergraph {

    /**
     * Construct a hypergraph from a collection of hyperedges encoded as
     * vertex set pairs.
     *
     * @param rawHyperedges a collection of hyperedges in (srcIds, dstIds) form
     * @param defaultValue the vertex attributes with which to create
     *                     vertices referenced by the hyperedges
     * @param uniqueHyperedges if multiple identical hyperedges are found
     *                         they are combined and the hyperedge
     *                         attribute is set to the sum. Otherwise
     *                         duplicate hyperedges are treated as
     *                         separate. To enable `uniqueEdges`,
     *                         a [[PartitionStrategy]] must be
     *                         provided.
     * @param hyperedgeStorageLevel the desired storage level at which to
     *                              cache the hyperedge if necessary
     * @param vertexStorageLevel the desired storage level at which to cache
     *                           the vertices if necessary
     * @tparam VD the vertex attribute type
     * @return a hypergraph with hyperedge attributes containing either the
     *         count of duplicate hyperedges or
     *         1 (if `uniqueHyperedges` is None) and vertex attributes
     *         containing the total degree of each
     *         vertex.
     */
    def fromHyperedgeTuples[VD: ClassTag](
         rawHyperedges: RDD[(VertexSet,VertexSet)],
         defaultValue: VD,
         uniqueHyperedges:
         Option[PartitionStrategy] = None,
         hyperedgeStorageLevel:StorageLevel = StorageLevel.MEMORY_ONLY,
         vertexStorageLevel:StorageLevel = StorageLevel.MEMORY_ONLY):
    Hypergraph[VD, Int] = {
        val hyperedges = rawHyperedges.map(p => Hyperedge(p._1, p._2, 1))
        val hypergraph = HypergraphImpl(hyperedges, defaultValue,
            hyperedgeStorageLevel, vertexStorageLevel)
        uniqueHyperedges match {
            case Some(p) => hypergraph.partitionBy(p).groupHyperedges((a,
                                                                       b) =>
                a + b)
            case None => hypergraph
        }
    }

    /**
     * Construct a hypergraph from a collection of hyperedges
     * @param hyperedges the RDD constaining the set of hyperedges in the
     *                   hypergraph
     * @param defaultValue the default vertex attribute to use for each vertex
     * @param hyperedgeStorageLevel the desired storage level at which to
     *                              cache the hyperedges if necessary
     * @param vertexStorageLevel the desired storage level at which to cache
     *                           the vertices if necessary
     * @tparam VD the vertex attribute type
     * @tparam ED the hyperedge attribute type
     * @return a hypergraph with hyperedge attributes described by
     *         `hyperedges` and vertices given
     *         by all vertices in `hyperedges` with value `defaultValue`
     */
    def fromHyperedges[VD: ClassTag, ED: ClassTag](
        hyperedges:RDD[Hyperedge[ED]],
        defaultValue: VD,
        hyperedgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        vertexStorageLevel:StorageLevel = StorageLevel.MEMORY_ONLY)
    : Hypergraph[VD, ED] = {
        HypergraphImpl(hyperedges, defaultValue, hyperedgeStorageLevel,
            vertexStorageLevel)
    }

    /**
     * Constrcut a hypergraph from a collection of vertices and hyperedges
     * with attributes.
     * Duplicate vertices are picked arbitrarily and vertices found in
     * hyperedge collection
     * but ont in the input vertices are assigned the default attribute value.
     *
     * @param vertices the collection of vertices and their attributes
     * @param hyperedges the collection of hyperedges in the hypergraph
     * @param defaultVertexAttr the default vertex attribute to use for
     *                          vertex mentioned in
     *                          the hyperedges but not in vertices
     * @param hyperedgeStorageLevel the desired storage level at which to cache the
     *                              hyperedges if necessary
     * @param vertexStorageLevel the desired storage level at which to cache the vertices
     *                           if necessary
     * @tparam VD the vertex attribute type
     * @tparam ED the hyperedge attribute type
     * @return a hypergraph with hyperedges and vertices specified by the input
     */
    def apply[VD: ClassTag, ED: ClassTag](
        vertices: RDD[(VertexId, VD)],
        hyperedges: RDD[Hyperedge[ED]],
        defaultVertexAttr: VD = null.asInstanceOf[VD],
        hyperedgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Hypergraph[VD, ED] = {
        HypergraphImpl(vertices, hyperedges, defaultVertexAttr,
            hyperedgeStorageLevel, vertexStorageLevel)
    }

    /**
     * Implicitly extracts the [[HypergraphOps]] member from a hypergraph
     */
    implicit def hypergraphToHypergraphOps[VD: ClassTag, ED: ClassTag]
    (h: Hypergraph[VD, ED]): HypergraphOps[VD, ED] = h.ops
}