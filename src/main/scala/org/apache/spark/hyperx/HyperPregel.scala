package org.apache.spark.hyperx

import org.apache.spark.{Accumulator, Logging}

import scala.reflect.ClassTag

/**
 * The implementation of Hypergraph Pregel computation framework
 *
 */
object HyperPregel extends Logging {

    def apply[VD: ClassTag, ED: ClassTag, A: ClassTag](
        hypergraph: Hypergraph[VD, ED], initialMsg: A,
        maxIterations: Int = Int.MaxValue,
        activeDirection: HyperedgeDirection = HyperedgeDirection.Either)(
        vprog: (VertexId, VD, A) => VD,
        hprog: HyperedgeTuple[VD, ED] => Iterator[(VertexId, A)],
        mergeMsg: (A, A) => A): Hypergraph[VD, ED] = {

        var h = hypergraph.mapVertices((vid, data) =>
            vprog(vid, data, initialMsg)).cache()

        var msg = h.mapReduceTuples(hprog, mergeMsg)

        var activeMsg = msg.count()

        var prevH: Hypergraph[VD, ED] = null
        var i = 0
        while (activeMsg > 0 && i < maxIterations) {
            val start = System.currentTimeMillis()

            // Compute the updated vertex attribute values based on the old
            // values and the messages reside on the partitions
            // PARTITION: this is not modeled, as which vertices would receive
            // messages and therefore need to be updated is determined on the
            // particular algorithm
            val newVerts = h.vertices.innerJoin(msg)(vprog).cache()
            prevH = h

            // Update the vertex attribute values
            // PARTITION: This is related to the number and the load balance
            // of replicas, fewer replicas suggest fewer vertex attributes need
            // to be updated
            h = h.outerJoinVertices(newVerts) {
                (vid, old,newOpt) => newOpt.getOrElse(old)
            }
            h.hyperedges.cache()
            h.vertices.cache()
            h.cache()

            val oldMsg = msg
            // Hyperedge computation
            // PARTITION: hyperedge degree balance
            msg = h.mapReduceTuples(hprog, mergeMsg, Some((newVerts,
                    activeDirection))).cache()
            activeMsg = msg.count()
            logInfo(("HYPERX DEBUGGING: S1 mapReduceTuple %d generating %d " +
                "messages %d ms")
                .format(i, activeMsg, System.currentTimeMillis() - start))

            // unpersist old hypergraphs, vertices, and messages
            oldMsg.unpersist(blocking = false)
            newVerts.unpersist(blocking = false)
            prevH.unpersistVertices(blocking = true)
            prevH.hyperedges.unpersist(blocking = true)
            i += 1
        }
        h
    }

    def run[VD: ClassTag, ED: ClassTag, A: ClassTag](
        hypergraph: Hypergraph[VD, ED], initialMsg: A,
        maxIterations: Int = Int.MaxValue,
        activeDirection: HyperedgeDirection = HyperedgeDirection.Either)(
        vprog: (VertexId, VD, A) => VD,
        hprog: (HyperedgeTuple[VD, ED], Accumulator[Int], Accumulator[Int]) => Iterator[(VertexId, A)],
        mergeMsg: (A, A) => A): Hypergraph[VD, ED] = {

        var h = hypergraph.mapVertices((vid, data) =>
            vprog(vid, data, initialMsg)).cache()

        var msg = h.mapReduceTuplesP(hprog, mergeMsg)

        var activeMsg = msg.count()

        var prevH: Hypergraph[VD, ED] = null
        var i = 0
        while (activeMsg > 0 && i < maxIterations) {
            val start = System.currentTimeMillis()

            // Compute the updated vertex attribute values based on the old
            // values and the messages reside on the partitions
            // PARTITION: this is not modeled, as which vertices would receive
            // messages and therefore need to be updated is determined on the
            // particular algorithm
            val innerStart = System.currentTimeMillis()
            val newVerts = h.vertices.innerJoin(msg)(vprog).cache()
            prevH = h
            newVerts.count()
            newVerts.cache()
            val inner = System.currentTimeMillis() - innerStart

            // Update the vertex attribute values
            // PARTITION: This is related to the number and the load balance
            // of replicas, fewer replicas suggest fewer vertex attributes need
            // to be updated
            val outerStart = System.currentTimeMillis()
            h = h.outerJoinVertices(newVerts) {
                (vid, old,newOpt) => newOpt.getOrElse(old)
            }
            h.hyperedges.cache()
            h.vertices.cache()
            h.cache()
            h.vertices.count()
            val outer = System.currentTimeMillis() - outerStart

            val oldMsg = msg
            // Hyperedge computation
            // PARTITION: hyperedge degree balance
            val mrStart = System.currentTimeMillis()
            msg = h.mapReduceTuplesP(hprog, mergeMsg, Some((newVerts,
                    activeDirection))).cache()
            activeMsg = msg.count()
            logInfo(("HYPERX DEBUGGING: S1 mapReduceTuple %d generating %d " +
                    "messages %d %d %d / %d ms")
                    .format(i, activeMsg,  inner, outer,
                        System.currentTimeMillis() - mrStart,
                        System.currentTimeMillis() - start))

            // unpersist old hypergraphs, vertices, and messages
            oldMsg.unpersist(blocking = false)
            newVerts.unpersist(blocking = false)
            prevH.unpersistVertices(blocking = true)
            prevH.hyperedges.unpersist(blocking = true)
            i += 1
        }
        h
    }
}
