package org.apache.spark.hyperx.impl

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.hyperx.{HyperedgeRDD, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Manages shipping vertex attributes to the hyperedge partitions of an
 * [[org.apache.spark.hyperx.HyperedgeRDD]]. Vertex attributes may be
 * partially shipped to
 * construct a tuple view with vertex attributes on only one side,
 * and they may be updated.
 * An active vertex set may additionally be shipped to the hyperedge
 * partitions. Be careful
 * not to store a reference to `hyperedges`, since it may be modified when
 * the attribute
 * shipping level is upgraded.
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */
private[hyperx]
class ReplicatedVertexView[VD: ClassTag, ED: ClassTag](
    var hyperedges: HyperedgeRDD[ED, VD], var hasSrcIds: Boolean = false,
    var hasDstIds: Boolean = false) extends Logging with Serializable {

    /**
     * Return a new `ReplicatedVertexView` with the specified `HyperedgeRDD`,
     * which must have
     * the same shipping level
     */
    def withHyperedges[VD2: ClassTag, ED2: ClassTag](
    hyperedges_ : HyperedgeRDD[ED2, VD2]):
    ReplicatedVertexView[VD2, ED2] = {
        new ReplicatedVertexView(hyperedges_, hasSrcIds, hasDstIds)
    }

    /**
     * Return a new `ReplicatedVertexView` where hyperedges are reversed and
     * shipping levels
     * are swapped to match.
     * @return
     */
    def reverse() = {
        val newHyperedges = hyperedges.mapHyperedgePartitions((pid, part) =>
            part.reverse)
        new ReplicatedVertexView(newHyperedges, hasDstIds, hasSrcIds)
    }

    /**
     * Upgrade the shipping level in-place to the specified levels by
     * shipping vertex attributes from
     * `vertices`. This operation modifies the `ReplicatedVertexView`,
     * and callers can access
     * `hyperedges` afterwards to obtain the upgraded view.
     */
    def upgrade(vertices: VertexRDD[VD], includeSrc: Boolean,
        includeDst: Boolean)
    : Unit = {
        val shipSrc = includeSrc && !hasSrcIds
        val shipDst = includeDst && !hasDstIds
        if (shipSrc || shipDst) {

            var start = System.currentTimeMillis()
            val shippedVerts: RDD[(Int, VertexAttributeBlock[VD])] =
                vertices.shipVertexAttributes(shipSrc, shipDst).setName(
                    ("ReplicatedVertexView.upgrade(%s, %s) - " +
                            "shippedVerts %s %s (broadcast)")
                        .format(includeSrc, includeDst, shipSrc, shipDst))
                    .partitionBy(hyperedges.partitioner.get)
            //logInfo("HYPERX DEBUGGING: S1.0.0 upgrade.shipVertexAttributes in %d ms".format(System.currentTimeMillis() - start))
            val sc = this.hyperedges.context
//            val performanceTracker = Array.fill(hyperedges.partitionsRDD.count().toInt)(sc.accumulator(0))
            start = System.currentTimeMillis()
            val newHyperedges: HyperedgeRDD[ED, VD] =
                hyperedges.withPartitionsRDD(
                    hyperedges.partitionsRDD.zipPartitions(shippedVerts) {
                        (hPartIter, shippedVertsIter) =>
                            val ret = hPartIter.map {
                                case (pid, hyperedgePartition) => {
//                                    val eachStart = System.currentTimeMillis()
                                    logInfo("HYPERX DEBUGGING: update vertices begins...")
                                    val newPartition = hyperedgePartition.updateVertices(shippedVertsIter.flatMap[(VertexId, VD)](_._2.iterator))
//                                    val newSize = newPartition.size
                                    logInfo("HYPERX DEBUGGING: update vertices ends")
//                                    performanceTracker(pid) += (System.currentTimeMillis() - eachStart).toInt
                                    (pid, newPartition)
                                }
                            }
                            ret
                        }
                )
//            val count = newHyperedges.count()
            //logInfo("HYPERX DEBUGGING: S1.0.1 upgrade.zipPartition in %d ms".format(System.currentTimeMillis() - start))
            hyperedges = newHyperedges
            hasSrcIds = includeSrc
            hasDstIds = includeDst
        }
    }

    /**
     * Return a new `ReplicatedVertexView` where the `activeSet` in each
     * hyperedge partition
     * contains only vertex ids present in `active`. This ships a vertex id
     * to all hyperedge partitions
     * where it is referenced, ignoring the attribute shipping level.
     */
    def withActiveSet(actives: VertexRDD[_]): ReplicatedVertexView[VD, ED] = {
        val shippedActives = actives.shipVertexIds()
                .setName("ReplicatedVertexView.withActiveSet - shippedActives" +
                " (broadcast)")
                .partitionBy(hyperedges.partitioner.get)

        val newHyperedges = hyperedges.withPartitionsRDD[ED,
                VD](hyperedges.partitionsRDD.zipPartitions(shippedActives) {
            (hPartIter, shippedActivesIter) => hPartIter.map {
                case (pid, hyperedgePartition) =>
                    (pid, hyperedgePartition.withActiveSet(shippedActivesIter
                            .flatMap(_._2.iterator)))
            }
        })
        new ReplicatedVertexView(newHyperedges, hasSrcIds, hasDstIds)
    }

    /**
     * Return a new `ReplicatedVertexView` where vertex attributes in
     * hyperedge partition are
     * updated using `updates`. This ships a vertex attribute only to the
     * hyperedge partitions where
     * it is in the position(s) specified by the attribute shipping level.
     */
    def updateVertices(updates: VertexRDD[VD]): ReplicatedVertexView[VD, ED] = {

        // send the updated vertex attribute values to its replica partitions
        val shippedVerts = updates.shipVertexAttributes(hasSrcIds, hasDstIds)
                .setName(("ReplicatedVertexView.updateVertices - " +
                "shippedVerts %s %s (broadcast)").format(hasSrcIds, hasDstIds))
                .partitionBy(hyperedges.partitioner.get)
        //logInfo("HYPERX DEBUGGING: REPLICA # of shippedVertices %d".format(shippedVerts.map(_._2.vids.size).reduce(_ + _)))
//        val eachTracker = Array.fill(hyperedges.partitionsRDD.count().toInt)(hyperedges.context.accumulator(0))
        // update the replicas in each partition, the load balance matters here
        val newHyperedges = hyperedges.withPartitionsRDD[ED, VD](
            hyperedges.partitionsRDD.zipPartitions(shippedVerts) {
            (hPartIter, shippedVertsIter) => hPartIter.map {
                case (pid, hyperedgePartition) =>
//                    val set = shippedVertsIter.toSet
//                    eachTracker(pid) += set.map(_._2.vids.size).reduce(_ + _)
                    (pid, hyperedgePartition.updateVertices(
                        shippedVertsIter.flatMap(_._2.iterator)))
            }
        })
//        newHyperedges.cache().count()
//        logDebug(("HYPERX DEBUGGING: REPLICA # of shipVertices to each partition " +
//            "%s with std %d ")
//            .format((0 until hyperedges.partitionsRDD.count().toInt).map(i => i + ": " + eachTracker(i).value.toString).reduce(_ + " " + _),
//                HyperUtils.dvt(eachTracker.map(_.value)).toInt))
//        newHyperedges.unpersist(blocking = false)
        new ReplicatedVertexView(newHyperedges, hasSrcIds, hasDstIds)
    }
}
