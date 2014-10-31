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
        hprog: (HyperedgeTuple[VD, ED], Accumulator[Int]) =>
                Iterator[(VertexId, A)],
        mergeMsg: (A, A) => A): Hypergraph[VD, ED] = {

        var h = hypergraph.mapVertices((vid, data) =>
            vprog(vid, data, initialMsg)).cache()

        val k = sc.getConf.get("hyperx.debug.k").toInt

        var msg = h.mapReduceTuplesP(sc,
            Array.fill(k)(sc.accumulator(0)), Array.fill(k)(
                sc.accumulator(0)), Array.fill(k)(sc.accumulator(0)),
                Array.fill(k)(sc.accumulator(0)),
                Array.fill(k)(sc.accumulator(0)),
                Array.fill(k)(sc.accumulator(0)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                Array.fill(k)(sc.accumulator(0L)),
                0L,
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
            val mT = Array.fill(k)(sc.accumulator(0))
            val cT = Array.fill(k)(sc.accumulator(0))
            val mcT = Array.fill(k)(sc.accumulator(0))
            val rT = Array.fill(k)(sc.accumulator(0))
            val sT = Array.fill(k)(sc.accumulator(0))
            val zT = Array.fill(k)(sc.accumulator(0))
            val sStart = Array.fill(k)(sc.accumulator(0L))
            val zStart = Array.fill(k)(sc.accumulator(0L))
            val mStart = Array.fill(k)(sc.accumulator(0L))
            val cStart = Array.fill(k)(sc.accumulator(0L))
            val rStart = Array.fill(k)(sc.accumulator(0L))
            val sComplete = Array.fill(k)(sc.accumulator(0L))
            val zComplete = Array.fill(k)(sc.accumulator(0L))
            val mComplete = Array.fill(k)(sc.accumulator(0L))
            val cComplete = Array.fill(k)(sc.accumulator(0L))
            val rComplete = Array.fill(k)(sc.accumulator(0L))
            val mrStart = System.currentTimeMillis()
            logInfo("HYPERX DEBUGGING: mrt starts" )
            msg = h.mapReduceTuplesP(sc, mT, cT, mcT, rT, sT, zT,
                mStart, cStart, rStart, sStart, zStart,
                mComplete, cComplete, rComplete, sComplete, zComplete, mrStart,
                hprog, mergeMsg, Some(x = (newVerts, activeDirection))).cache()
            val scanTime = System.currentTimeMillis() - mrStart
            activeMsg = msg.count()
            val mVals = mT.map(_.value)
            val cVals = cT.map(_.value)
            val mcVals = mcT.map(_.value)
            val rVals = rT.map(_.value)
            val sVals = sT.map(_.value)
            val zVals = zT.map(_.value)

            logInfo("HYPERX DEBUGGING: %d generates %d messages in %d ms"
                .format(i, activeMsg, (System.currentTimeMillis() - start).toInt))
            logInfo("HYPERX DEBUGGING: inner %d outer %d mrt %d"
                .format(inner, outer, (System.currentTimeMillis() - mrStart).toInt))
            logInfo("HYPERX DEBUGGING: scan: %d scheduling and shipping tasks: %d".format(scanTime, - mrStart + sStart.map(_.value).min - scanTime))
//            logInfo("HYPERX DEBUGGING: mrt 1 active set: starts %d completes %d"
//                    .format(- mrStart + sStart.map(_.value).min, - mrStart + sComplete.map(_.value).max))
            logInfo("HYPERX DEBUGGING: active set (pipeline): %d".format(sComplete.map(_.value).max - sStart.map(_.value).min))
            logInfo("HYPERX DEBUGGING: active set details : ship avg %d min %d max %d std %d zip avg %d min %d max %d std %d"
                    .format(HyperUtils.avg(sVals).toInt, sVals.min, sVals.max, HyperUtils.dvt(sVals).toInt,
                        HyperUtils.avg(zVals).toInt, zVals.min, zVals.max, HyperUtils.dvt(zVals).toInt))
            logInfo("HYPERX DEBUGGING: map-combine (pipeline): %d starts %d completes %d avg %d min %d max %d std %d"
                .format(cComplete.map(_.value).max - cStart.map(_.value).min, - mrStart + cStart.map(_.value).min, - mrStart + cComplete.map(_.value).max, HyperUtils.avg(cVals).toInt, cVals.min, cVals.max, HyperUtils.dvt(cVals).toInt))
            logInfo("HYPERX DEBUGGING: reduce (pipeline): %d starts %d completes %d avg %d min %d max %d std %d"
                .format(rComplete.map(_.value).max - rStart.map(_.value).min, - mrStart + rStart.map(_.value).min, - mrStart + rComplete.map(_.value).max, HyperUtils.avg(rVals).toInt, rVals.min, rVals.max, HyperUtils.dvt(rVals).toInt))

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
