package org.apache.spark.hyperx

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.{SparkContext, Accumulator, Logging}
import org.apache.spark.SparkContext._

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
        sc: SparkContext,
        hypergraph: Hypergraph[VD, ED], initialMsg: A,
        maxIterations: Int = Int.MaxValue,
        activeDirection: HyperedgeDirection = HyperedgeDirection.Either)(
        vprog: (VertexId, VD, A) => VD,
        hprog: (HyperedgeTuple[VD, ED], Accumulator[Int]) => Iterator[(VertexId, A)],
        mergeMsg: (A, A) => A): Hypergraph[VD, ED] = {

        var h = hypergraph.mapVertices((vid, data) =>
            vprog(vid, data, initialMsg)).cache()

        val k = sc.getConf.get("hyperx.debug.k").toInt

        var msg = h.mapReduceTuplesP(sc,
            Array.fill(k)(sc.accumulator(0)), Array.fill(k)(sc.accumulator(0)), Array.fill(k)(sc.accumulator(0)), Array.fill(k)(sc.accumulator(0)),
            hprog, mergeMsg)

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
            val mT = Array.fill(k)(sc.accumulator(0))
            val cT = Array.fill(k)(sc.accumulator(0))
            val mcT = Array.fill(k)(sc.accumulator(0))
            val rT = Array.fill(k)(sc.accumulator(0))
            logInfo("HYPERX DEBUGGING: MRT begins now...")
            msg = h.mapReduceTuplesP(sc, mT, cT, mcT, rT, hprog, mergeMsg, Some((newVerts,
                    activeDirection))).cache()
            activeMsg = msg.count()
            logInfo("HYPERX DEBUGGING: MRT ends")
            val mVals = mT.map(_.value)
            val cVals = cT.map(_.value)
            val mcVals = mcT.map(_.value)
            val rVals = rT.map(_.value)
            logInfo(("HYPERX DEBUGGING: S1 mapReduceTuple %d generating %d " +
                    "messages inner %d outer %d mr %d (map %d %d combine %d %d mc %d %d reduce %d %d) / %d ms")
                    .format(i, activeMsg,  inner, outer,
                (System.currentTimeMillis() - mrStart).toInt,
                        HyperUtils.avg(mVals).toInt, HyperUtils.dvt(mVals).toInt,
                        HyperUtils.avg(cVals).toInt, HyperUtils.dvt(cVals).toInt,
                        HyperUtils.avg(mcVals).toInt, HyperUtils.dvt(mcVals).toInt,
                        HyperUtils.avg(rVals).toInt, HyperUtils.dvt(rVals).toInt,
                (System.currentTimeMillis() - start).toInt))

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
