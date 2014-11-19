package org.apache.spark.hyperx

import org.apache.spark.Logging
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Contains additional functionality for [[Hypergraph]]. All operations are
 * expressed in terms of the
 * efficient HyperX API. This class is implicitly constructed fro each
 * Hypergraph object.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the hyperedge attribute type
 *
 *            Forked from GraphX 2.10, modified by Jin Huang
 */
class HypergraphOps[VD: ClassTag, ED: ClassTag](hypergraph: Hypergraph[VD,
        ED]) extends Serializable with Logging {

    /** The number of hyperedges in the hypergraph. */
    @transient lazy val numHyperedges: Long = hypergraph.hyperedges.partitionsRDD.map(p => p._2.data.size).reduce(_ + _)

    /** The number of vertices in the hypergraph. */
    @transient lazy val numVertices: Long = hypergraph.vertices.count()

    /**
     * The in-degree of each vertex in the hypergraph.
     * @note vertices with no in-hyperedges are not returned in the resulting
     *       RDD.
     * @note this could be inefficient as we need to union the sets of
     *       neighboring
     *       vertices obtained from every hyperedge
     */
    @transient lazy val inDegrees: VertexRDD[Int] =
        degreesRDD(HyperedgeDirection.In).setName("HypergraphOps.inDegrees")

    @transient lazy val inIncidents: VertexRDD[Int] =
        incidentRDD(HyperedgeDirection.In).setName("HypergraphOps.inIncidents")

    @transient lazy val inHDegrees: RDD[(HyperedgeId, Int)] =
        hyperedgeDegreeRDD(HyperedgeDirection.In).setName("HypergraphOps.inHDegrees")

    /**
     * The out-degree of each vertex in the hypergraph
     * @note this could be inefficient as we need to union the sets of
     *       neighboring
     *       vertices obtained from every hyperedge
     */
    @transient lazy val outDegrees: VertexRDD[Int] =
        degreesRDD(HyperedgeDirection.Out).setName(
            "HypergraphOps.outDegrees")


    @transient lazy val outIncidents: VertexRDD[Int] =
        incidentRDD(HyperedgeDirection.Out).setName("" +
            "HypergraphOps.outIncidents")

    @transient lazy val outHDegrees: RDD[(HyperedgeId, Int)] =
        hyperedgeDegreeRDD(HyperedgeDirection.Out).setName("HypergraphOps.outHDegrees")


    /**
     * The degree of each vertex in the graph
     * @note this could be inefficient as we need to union the sets of
     *       neighboring
     *       vertices obtained from every hyperedge
     */
    @transient lazy val degrees: VertexRDD[Int] =
        degreesRDD(HyperedgeDirection.Either).setName("HypergraphOps.degrees")

    @transient lazy val incidents: VertexRDD[Int] =
        incidentRDD(HyperedgeDirection.Either).setName(
            "HypergraphOps.incidents")


    @transient lazy val hDegrees: RDD[(HyperedgeId, Int)] =
        hyperedgeDegreeRDD(HyperedgeDirection.Either).setName("HypergraphOps.hDegrees")

    @transient lazy val laplacian: RDD[(VertexId, (Array[VertexId], Array[Double]))] =
        evaluateLaplacian()

    /**
     * Collect the neighbor vertex ids for each vertex
     * @note todo size or ids?
     * @param hyperedgeDirection the direction along which to collect
     *                           neighbor vertices
     * @return the set of neighboring ids for each vertex
     */
    def collectNeighborIds(hyperedgeDirection: HyperedgeDirection):
    VertexRDD[BitSet] = {
        hypergraph.mapReduceTuples[BitSet](ht => degreesIterator(ht,
            hyperedgeDirection), _ | _)
    }

    /**
     * Returns an iterator for counting the degrees of neighboring vertices
     * on one hyperedge
     * @param hyperedgeTuple the hyperedge that the iterator is created on
     * @param hyperedgeDirection the direction along which the degree is counted
     * @return an iterator on pairs of type (VertexId, VertexSet)
     */
    private def degreesIterator(hyperedgeTuple: HyperedgeTuple[VD, ED],
        hyperedgeDirection: HyperedgeDirection):Iterator[(VertexId, BitSet)] = {
        hyperedgeDirection match {
            case HyperedgeDirection.In =>
                hyperedgeTuple.dstAttr.keySet.iterator.map(v =>
                    (v, hyperedgeTuple.srcAttr.keySet.getBitSet)).toIterator
            case HyperedgeDirection.Out =>
                hyperedgeTuple.srcAttr.keySet.iterator.map(v =>
                    (v, hyperedgeTuple.dstAttr.keySet.getBitSet)).toIterator
            case HyperedgeDirection.Either | HyperedgeDirection.Both =>
                hyperedgeTuple.dstAttr.keySet.iterator.map(v =>
                    (v, hyperedgeTuple.srcAttr.keySet.getBitSet)).toIterator ++
                hyperedgeTuple.srcAttr.keySet.iterator.map(v =>
                    (v, hyperedgeTuple.dstAttr.keySet.getBitSet)).toIterator
        }
    }

    /**
     * Collect the neighbor vertex attributes for each vertex
     * @note this could be inefficient as we need to concatenate hashmaps of
     *       vertexId-attribute
     *       obtained from every hyperedge
     * @param hyperedgeDirection the direction along which to collect
     *                           neighboring vertices
     * @return the vertex set of neighboring vertex attributes for each vertex
     */
    def collectNeighbors(hyperedgeDirection: HyperedgeDirection)
    : VertexRDD[HyperAttr[VD]] = {
        hypergraph.mapReduceTuples[HyperAttr[VD]](ht => attrIterator(ht,
            hyperedgeDirection), _ ++ _)
    }

    /**
     * Returns an iterator for accessing the vertex attributes of neighboring
     * vertices on one hyperedge
     */
    private def attrIterator(hyperedgeTuple: HyperedgeTuple[VD, ED],
        hyperedgeDirection: HyperedgeDirection)
    :Iterator[(VertexId, HyperAttr[VD])] = {
        hyperedgeDirection match {
            case HyperedgeDirection.In =>
                hyperedgeTuple.dstAttr.keySet.iterator.map(
                    (_, hyperedgeTuple.srcAttr))
            case HyperedgeDirection.Out =>
                hyperedgeTuple.srcAttr.keySet.iterator.map(
                    (_, hyperedgeTuple.dstAttr))
            case HyperedgeDirection.Either | HyperedgeDirection.Both =>
                hyperedgeTuple.dstAttr.keySet.iterator.map(
                    (_, hyperedgeTuple.srcAttr)) ++
                hyperedgeTuple.srcAttr.keySet.iterator.map(
                    (_, hyperedgeTuple.dstAttr))
        }
    }

    /**
     * Collect the incident hyperedges for each vertex
     * @note this could be inefficient as we need to concatenate the arrays
     *       of hyperedges obtained
     *       from every hyperedge
     * @param hyperedgeDirection the direction along which to collect the
     *                           local hyperedges of vertices
     * @return the local hyperedges for each vertex
     */
    def collectHyperedges(hyperedgeDirection: HyperedgeDirection)
    : VertexRDD[Array[Hyperedge[ED]]] = {
        hypergraph.mapReduceTuples[Array[Hyperedge[ED]]](ht =>
            hyperedgeIterator(ht, hyperedgeDirection), _ ++ _)
    }

    /**
     * Returns an iterator for accessing the hyperedges attributes for one
     * hyperedges
     * @param hyperedgeTuple the hyperedge that the iterator is created on
     * @param hyperedgeDirection the direction along which the hyperedge
     *                           attributes are accessed
     * @return an iterator on hyperedges
     */
    private def hyperedgeIterator(hyperedgeTuple: HyperedgeTuple[VD, ED],
        hyperedgeDirection: HyperedgeDirection)
    : Iterator[(VertexId, Array[Hyperedge[ED]])] = {

        hyperedgeDirection match {
            case HyperedgeDirection.In =>
                hyperedgeTuple.dstAttr.keySet.iterator.map(v =>
                    (v, Array(new Hyperedge(hyperedgeTuple.srcAttr.keySet,
                        hyperedgeTuple.dstAttr.keySet, hyperedgeTuple.attr))))
            case HyperedgeDirection.Out =>
                hyperedgeTuple.srcAttr.keySet.iterator.map(v =>
                    (v, Array(new Hyperedge(hyperedgeTuple.srcAttr.keySet,
                        hyperedgeTuple.dstAttr.keySet, hyperedgeTuple.attr))))
            case HyperedgeDirection.Either | HyperedgeDirection.Both =>
                hyperedgeTuple.dstAttr.keySet.iterator.map(v =>
                    (v, Array(new Hyperedge(hyperedgeTuple.srcAttr.keySet,
                        hyperedgeTuple.dstAttr.keySet, hyperedgeTuple.attr)))
                ) ++
                hyperedgeTuple.srcAttr.keySet.iterator.map(v =>
                    (v, Array(new Hyperedge(hyperedgeTuple.srcAttr.keySet,
                        hyperedgeTuple.dstAttr.keySet, hyperedgeTuple.attr))))
        }
    }

    private def incidentIterator(hyperedgeTuple: HyperedgeTuple[VD, ED],
        hyperedgeDirection: HyperedgeDirection)
    :Iterator[(VertexId, Int)] = {
        hyperedgeDirection match {
            case HyperedgeDirection.In =>
                hyperedgeTuple.dstAttr.keySet.iterator.map((_, 1))
            case HyperedgeDirection.Out =>
                hyperedgeTuple.srcAttr.keySet.iterator.map((_, 1))
            case HyperedgeDirection.Both | HyperedgeDirection.Either =>
                hyperedgeTuple.srcAttr.keySet.iterator.map((_,1)) ++
                hyperedgeTuple.dstAttr.keySet.iterator.map((_,1))
        }
    }

    /**
     * Join the vertices with an RDD and then apply a function from the
     * vertex and RDD entry to a new
     * vertex value. The input table should contain at most one entry for
     * each vertex. If no entry is
     * provided the map function is skipped and the old value is used.
     */
//    def joinVertices[U: ClassTag](table: RDD[(VertexId,
//            U)])(mapFunc: (VertexId, VD, U) => VD): Hypergraph[VD, ED] = {
//        //TODO: Implementation
//        null.asInstanceOf[Hypergraph[VD, ED]]
//    }

    /**
     * Filter the hypergraph by computing some values to filter on,
     * and applying the predicates.
     * @param preprocess a function to compute new vertex and hyperedge
     *                   attributes before filtering
     * @param hpred hyperedge predicate to filter on after preprocess,
     *              see more details under
     *              [[org.apache.spark.hyperx.Hypergraph# s u b g r a p h]]
     * @param vpred vertex predicate to filter on after preprocess,
     *              see more details under
     *              [[org.apache.spark.hyperx.Hypergraph# s u b g r a p h]]
     * @tparam VD2 the vertex attribute type the predicate operates on
     * @tparam ED2 the hyperedge attribute type the predicate operates on
     * @return a subgraph of the original hypergraph, with its data unchanged
     */
    def filter[VD2: ClassTag, ED2: ClassTag](
        preprocess: Hypergraph[VD, ED] => Hypergraph[VD2, ED2],
        hpred: (HyperedgeTuple[VD2, ED2]) =>
            Boolean = (x: HyperedgeTuple[VD2, ED2]) => true,
        vspred: HyperAttr[VD2] => Boolean,
        vpred: (VertexId, VD2) =>
            Boolean =(v: VertexId, d: VD2) => true)
    : Hypergraph[VD, ED] = {
        hypergraph.mask(preprocess(hypergraph).subgraph(hpred, vspred, vpred))
    }

    /**
     * Picks a random vertex from the graph and returns its ID.
     */
    def pickRandomVertices(): VertexId = {
        val probability = 5000.0 / hypergraph.numVertices
        var found = false
        var retVal: VertexId = null.asInstanceOf[VertexId]
        while (!found) {
            val selectedVertices = hypergraph.vertices.flatMap { vidVvals =>
                if (Random.nextDouble() < probability) {
                    Some(vidVvals._1)
                }
                else {
                    None
                }
            }
            if (selectedVertices.count > 1) {
                found = true
                val collectedVertices = selectedVertices.collect()
                retVal = collectedVertices(Random.nextInt(collectedVertices
                        .size))
            }
        }
        retVal
    }

    def pickRandomVertices(num: Int): mutable.HashSet[VertexId] = {
        val probability = num * 1.0 / hypergraph.numVertices
        val retSet = new mutable.HashSet[VertexId]
        if (probability > 0.5) {
            hypergraph.vertices.map(_._1).collect().foreach{v =>
                if (Random.nextDouble() < probability) {
                    retSet.add(v)
                }
            }
        }
        else {
            while (retSet.size < num) {
                val selectedVertices = hypergraph.vertices.flatMap { vidVvals =>
                    if (Random.nextDouble() < probability) {
                        Some(vidVvals._1)
                    }
                    else {
                        None
                    }
                }
                if (selectedVertices.count > 1) {
                    val collectedVertices = selectedVertices.collect()
                    collectedVertices.foreach(retSet.add)
                }
            }
        }
        retSet
    }

    /**
     * Execute a Pregel-like iterative vertex-hyperedge-parallel abstraction.
     * The user-defined
     * vertex-program `vprog` is executed in parallel on each vertex
     * receiving any inbound messages
     * and computing a new value for the vertex. The user-defined
     * hyperedge-program `sendMsg`
     * is executed on all out-hyperedges and is used to compute optional
     * messages to the
     * destination vertices. The `mergeMsg` function is a commutative and
     * associative function
     * used to combine messages destined to the same vertex.
     *
     * On the first iteration all vertices receive the `initialMsg` and on
     * the subsequent
     * iteration if a vertex does not receive a message then the
     * vertex-program is not executed.
     *
     * This procedure iterates until there is no remaining messages or for
     * `maxIterations`
     * iterations.
     *
     * @param initialMsg the initial message each vertex receive at the
     *                   beginning
     * @param maxIterations the maximum number of iterations the procedure
     *                      could iterate
     * @param activeDirection the direction of hyperedge incident to a vertex
     *                        that received
     *                        a message in the previous round on which to run
     *                        `sendMsg`. For
     *                        example, if it is `HyperedgeDirection.Out`,
     *                        on out-hyperedges
     *                        of vertices that received a message in the
     *                        previous iteration
     *                        will run.
     * @param vprog the user-defined vertex program which runs on each vertex
     *              and receives the
     *              inbound message and computes a new vertex value. On the
     *              first iteration the
     *              vertex program is invoked on all vertices and is passed
     *              the default message.
     *              On subsequent iterations the vertex program is only
     *              invoked on those vertices
     *              that receive messages.
     * @param sendMsg a user-defined hyperedge program which runs on each
     *                hyperedges incident
     *                to vertices that received messages in the current
     *                iteration, which computes
     *                the optional messages to be delivered to the destined
     *                vertices.
     * @param mergeMsg a user-defined function that takes two incoming
     *                 messages of type A and
     *                 merges them into one single message of type A. This
     *                 function must be
     *                 commutative and associative and ideally the size of A
     *                 should not increase.
     * @tparam A the message type
     * @return the resulting hypergraph at the end of the computation
     */
    def hyperPregel[A: ClassTag](
        initialMsg: A, maxIterations: Int = Int.MaxValue,
        activeDirection: HyperedgeDirection = HyperedgeDirection.Either)(
        vprog: (VertexId, VD, A) => VD,
        sendMsg: HyperedgeTuple[VD,ED] => Iterator[(VertexId, A)],
        mergeMsg: (A, A) => A): Hypergraph[VD, ED] = {

        HyperPregel(hypergraph, initialMsg, maxIterations,activeDirection)(
            vprog, sendMsg, mergeMsg)
    }

    private def degreesRDD(hyperedgeDirection: HyperedgeDirection):
    VertexRDD[Int] = {
        hypergraph.mapReduceTuples[BitSet]({tuple =>
            degreesIterator(tuple, hyperedgeDirection)
        }, _ | _).mapValues(_.cardinality())
    }

    private def incidentRDD(hyperedgeDirection: HyperedgeDirection)
    : VertexRDD[Int] = {
        hypergraph.mapReduceTuples[Int]({tuple =>
            incidentIterator(tuple, hyperedgeDirection)
        }, _ + _)
    }

    private def hyperedgeDegreeRDD(hyperedgeDirection: HyperedgeDirection):
    RDD[(HyperedgeId, Int)] = {
        hypergraph.hyperedges.zipWithUniqueId().map{h =>
            hyperedgeDirection match {
                case HyperedgeDirection.In =>
                    (h._2.toInt, h._1.srcIds.size)
                case HyperedgeDirection.Out =>
                    (h._2.toInt, h._1.dstIds.size)
                case HyperedgeDirection.Both | HyperedgeDirection.Either =>
                    (h._2.toInt, h._1.srcIds.size + h._1.dstIds.size)
            }
        }
    }

    private def evaluateLaplacian(): RDD[(VertexId, (Array[VertexId], Array[Double]))] = {

        // sqrt diagonal matrix
        val D = hypergraph.outerJoinVertices(hypergraph.incidents){(vid, vdata, deg) =>
            deg match {
                case someDeg: Some[Int] =>
                    1.0 / Math.sqrt(someDeg.get)
                case None =>
                    0.0
            }}

        // multiplied by incident matrix
        // essentially the tuple view (as a tuple corresponds to a hyperedge with
        // its incident vertices), no need to materialize
        val DH = D

        // multiplied by hyperedge weight matrix
        // essentially scale the vertex values by the hyperedge weight
        // no need to materialize
        val DHW = DH

        var start = System.currentTimeMillis()
        // multiplied by the transpose of incident matrix
        // for each pair of vertices co-appear in at least one hyperedge, compute
        // the sum of values on all the co-appearing hyperedges
        // essentially aggregate by neighboring pair
        val DHWH = DHW.mapReduceTuples[HyperXOpenHashMap[VertexId, Double]]({tuple =>

            // just in case ED is not double
            val weight = tuple.attr match {
                case doubleWeight: Double =>
                    doubleWeight
                case _ =>
                    1.0
            }

            // generate co-appearance pairs, routed to one of the vertices while avoiding
            // duplicate computations on symmetry pairs
            val data = tuple.srcAttr.mapOn(_ * weight).iterator ++ tuple.dstAttr.mapOn(_ * weight)
            (tuple.srcAttr.keySet.iterator ++ tuple.dstAttr.keySet.iterator).map(u =>
                (u, HyperXOpenHashMap(data.filter{v =>
                    val vid = v._1
                    if ((u + vid) % 2 ==0) {
                        u < vid
                    } else {
                        u >= vid
                    }
                }))
            )
        }, {(a, b) =>
            // update in place on a
            b.iterator.foreach{pair =>
                accu(a, pair._1, pair._2)
            }
            a
        }).cache()
        val countDHWH = DHWH.count()
        logInfo("HYPERX DEBUGGING: computed DHWH for %d in %d".format(countDHWH, System.currentTimeMillis() - start))
        start = System.currentTimeMillis()


        // multiplied by degree matrix
        // scale each pair value by its corresponding degree
//        val localIncident = D.vertices.filter(v => v._2 > 0.0).map(v => (v._1.toInt, v._2)).collect().toMap
//        val bcIncident = DHWH.context.broadcast(localIncident)
        val laplacian = DHWH.map{each =>
            val arrays = each._2.mapOn({(vid, d) =>
                //val scale = d * bcIncident.value(vid.toInt)
                val scale = d
                if (vid == each._1) {
                    1.0 - scale
                } else {
                    0.0 - scale
                }
            }).toArrays
            (each._1, arrays)
        }
        laplacian
    }

    private def accu(map: HyperXOpenHashMap[VertexId, Double], key: VertexId, value: Double): Unit = {
        if (map.hasKey(key)) {
            map.update(key, map(key) + value)
        } else {
            map.update(key, value)
        }
    }
}
