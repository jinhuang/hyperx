package org.apache.spark.hyperx

import org.apache.spark.SparkContext._
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.{Accumulator, Logging, SparkContext}

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
//            val newVerts = h.vertices
            prevH = h

            // Update the vertex attribute values
            // PARTITION: This is related to the number and the load balance
            // of replicas, fewer replicas suggest fewer vertex attributes need
            // to be updated
            h = h.outerJoinVertices(newVerts) {
                (vid, old,newOpt) => newOpt.getOrElse(old)
            }.cache()

            val oldMsg = msg

            // Hyperedge computation
            // PARTITION: hyperedge degree balance
            msg = h.mapReduceTuples(hprog, mergeMsg, Some((newVerts,
                    activeDirection))).cache()
            activeMsg = msg.count()
            logInfo("HYPERX DEBUGGING: %d generates %d messages in %d ms"
                .format(i, activeMsg,
                    (System.currentTimeMillis() - start).toInt))

            // unpersist old hypergraphs, vertices, and messages
            oldMsg.unpersist(blocking = false)
            newVerts.unpersist(blocking = false)
            prevH.unpersistVertices(blocking = false)
            prevH.hyperedges.unpersist(blocking = false)
            i += 1
        }
        h
    }

    /** The debug version with various performance logs */
    def run[VD: ClassTag, ED: ClassTag, A: ClassTag](
        sc: SparkContext,
        hypergraph: Hypergraph[VD, ED], initialMsg: A,
        maxIterations: Int = Int.MaxValue,
        activeDirection: HyperedgeDirection = HyperedgeDirection.Either)(
        vprog: (VertexId, VD, A) => VD,
        hprog: (HyperedgeTuple[VD, ED], Accumulator[Int], Accumulator[Int],
            Accumulator[Int], Accumulator[Int]) => Iterator[(VertexId, A)],
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
                Array.fill(k)(sc.accumulator(0)),
                Array.fill(k)(sc.accumulator(0)),
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
//            newVerts.count()
            prevH = h
            val inner = System.currentTimeMillis() - innerStart

            // Update the vertex attribute values
            // PARTITION: This is related to the number and the load balance
            // of replicas, fewer replicas suggest fewer vertex attributes need
            // to be updated
            val outerStart = System.currentTimeMillis()
            h = h.outerJoinVertices(newVerts) {
                (vid, old,newOpt) => newOpt.getOrElse(old)
            }.cache()
            val outer = System.currentTimeMillis() - outerStart

            val oldMsg = msg
            // Hyperedge computation
            // PARTITION: hyperedge degree balance
            val msT = Array.fill(k)(sc.accumulator(0))
            val mdT = Array.fill(k)(sc.accumulator(0))
            val msdT = Array.fill(k)(sc.accumulator(0))
            val mddT = Array.fill(k)(sc.accumulator(0))
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
            val rCount = Array.fill(k)(sc.accumulator(0))
            val cCount = Array.fill(k)(sc.accumulator(0))
            val mrStart = System.currentTimeMillis()
//            logInfo("HYPERX DEBUGGING: mrt starts" )
            msg = h.mapReduceTuplesP(sc, msT, mdT, msdT, mddT, cT, mcT, rT,
                sT, zT, mStart, cStart, rStart, sStart, zStart,
                mComplete, cComplete, rComplete, rCount, cCount, sComplete,
                zComplete, mrStart,
                hprog, mergeMsg, Some(x = (newVerts, activeDirection))).cache()
            val scanTime = System.currentTimeMillis() - mrStart
            activeMsg = msg.count()
            val msVals = msT.map(_.value)
            val mdVals = mdT.map(_.value)
            val mVals = (0 until k).map(i => msVals(i) + mdVals(i)).toArray
            val msdVals = msdT.map(_.value)
            val mddVals = mddT.map(_.value)
            val cVals = cT.map(_.value)
            val ccVals = cCount.map(_.value)
            val rVals = rT.map(_.value)
            val rcVals = rCount.map(_.value)
            val sVals = sT.map(_.value)
            val zVals = zT.map(_.value)
            val cDuration = (0 until k).map(i =>
                (cComplete(i).value - cStart(i).value).toInt).toArray

            logInfo("HYPERX DEBUGGING: %d generates %d messages in %d ms"
                .format(i, activeMsg,
                    (System.currentTimeMillis() - start).toInt))
            logInfo("HYPERX DEBUGGING: inner %d outer %d mrt %d"
                .format(inner, outer,
                    (System.currentTimeMillis() - mrStart).toInt))
            logInfo(("HYPERX DEBUGGING: scan: %d scheduling and " +
                "shipping tasks: %d").format(
                    scanTime, - mrStart + sStart.map(_.value).min - scanTime))
            logInfo("HYPERX DEBUGGING: active set (pipeline): %d"
                .format(sComplete.map(_.value).max - sStart.map(_.value).min))
            logInfo(("HYPERX DEBUGGING: active set details : ship avg" +
                " %d min %d max %d std %d zip avg %d min %d max %d std" +
                " %d std percent %f")
                    .format(HyperUtils.avg(sVals).toInt, sVals.min, sVals.max,
                    HyperUtils.dvt(sVals).toInt, HyperUtils.avg(zVals).toInt,
                    zVals.min, zVals.max, HyperUtils.dvt(zVals).toInt,
                    HyperUtils.dvt(zVals) / HyperUtils.avg(zVals)))
            logInfo(("HYPERX DEBUGGING: map (pipeline): %d starts %d completes" +
                " %d avg %d min %d max %d std %d std percent %f")
                .format(mComplete.map(_.value).max - mStart.map(_.value).min,
                    - mrStart + mStart.map(_.value).min,
                    - mrStart + mComplete.map(_.value).max,
                    HyperUtils.avg(mVals).toInt, mVals.min, mVals.max,
                    HyperUtils.dvt(mVals).toInt,
                    HyperUtils.dvt(mVals) / HyperUtils.avg(mVals)))
            logArray("map (details) src", msVals)
            logArray("map (details) dst", mdVals)
            logArray("map (details) srcDegree", msdVals)
            logArray("map (details) dstDegree", mddVals)
            logArray("combine", cDuration)
            logArray("combine (details) vertex count", ccVals)
            logInfo(("HYPERX DEBUGGING: reduce (pipeline): %d starts %d " +
                "completes %d avg %d min %d max %d std %d std percent %f")
                .format(rComplete.map(_.value).max - rStart.map(_.value).min,
                    - mrStart + rStart.map(_.value).min,
                    - mrStart + rComplete.map(_.value).max,
                    HyperUtils.avg(rVals).toInt, rVals.min, rVals.max,
                    HyperUtils.dvt(rVals).toInt,
                    HyperUtils.dvt(rVals) / HyperUtils.avg(rVals)))
            logArray("reduce (details) vertex count", rcVals)

            // unpersist old hypergraphs, vertices, and messages
            oldMsg.unpersist(blocking = false)
            newVerts.unpersist(blocking = false)
            prevH.unpersistVertices(blocking = true)
            prevH.hyperedges.unpersist(blocking = true)
            i += 1
        }
        h
    }

    private def logArray(name: String, ary: Array[Int]): Unit = {
        logInfo("HYPERX DEBUGGING: %s avg %d min %d max %d std %d std percent %f"
                .format(name, HyperUtils.avg(ary).toInt, ary.min,
                ary.max, HyperUtils.dvt(ary).toInt,
                HyperUtils.dvt(ary) / HyperUtils.avg(ary)))
    }
}
