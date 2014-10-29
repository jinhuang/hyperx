package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.hyperx.{PartitionId, VertexId}
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet}

/**
 *
 */
abstract class GreedySerialPartition extends SerialPartition {

    private[partition] def addHyperedge(h: String)
    : PartitionId = {
        val extraR = (0 until k).map {i =>
            HyperUtils.iteratorFromHString(h).count(v =>
                !locals(i).contains(v) && !hasDemand(i, v))}.toArray
        val extraDm = (0 until k).map {i =>
            HyperUtils.iteratorFromHString(h).count(v => !hasDemand(i, v))
        }
        val newPid = (0 until k)
            .map(i =>
                (i, costAddHyperedge(h, i, extraR(i), extraDm(i))))
            .minBy(_._2)._1
        assign(h, newPid)
        addMaterialized(h, newPid)
        newPid
    }

    private[partition] def moveHyperedge(h: (String, PartitionId))
    : PartitionId = {
        val otherDemands = otherDemand(h._2, h._1)
        val aux = Array.fill(k)(
            new OpenHashMap[
                VertexId, (Boolean, Boolean, Boolean, Boolean)]())
        HyperUtils.iteratorFromHString(h._1).foreach { v =>
            val residency = vertices(v)
            val assignedToFrom = residency == h._2
            val soleDemandToFrom = !otherDemands.contains(v)
            (0 until k).foreach(to =>
                aux(to).update(v,
                    (residency == to, hasDemand(to, v), soleDemandToFrom,
                        assignedToFrom))
            )
        }
        val newPid = (0 until k)
            .map(i => (i, costMoveHyperedge(h._1, h._2, i, aux(i))))
            .minBy(_._2)._1
        assign(h._1, newPid)
        moveMaterialized(h._1, h._2, newPid, otherDemands)
        newPid
    }

    private[partition] def addVertex(v: VertexId): PartitionId = {
        // a sub heuristics to balance the number of vertices on partitions
        val costPairs = (0 until k).map(i => (i, costAddVertex(v, i, hasDemand(i, v))))
        val minCost = costPairs.minBy(_._2)._2
        val newPid = costPairs.filter(p => (p._2 - minCost) <= 0.00001)
                .map(p => (p._1, locals(p._1).size)).minBy(_._2)._1

        assign(v, newPid)
        addMaterialized(v, newPid)
        newPid
    }

    private[partition] def moveVertex(v: (VertexId, PartitionId))
    : PartitionId = {
        val relevant = (0 until k).map(i => hasDemand(i, v._1))
        val costPairs = (0 until k).map(i => (i, costMoveVertex(v._1, v._2, i, relevant(v._2), relevant(i))))
        val minCost = costPairs.minBy(_._2)._2
        val newPid = costPairs.filter(p => (p._2 - minCost) <= 0.00001).map(p => (p._1, locals(p._1).size)).minBy(_._2)._1
        assign(v._1, newPid)
        moveMaterialized(v._1, v._2, newPid)
        newPid
    }

    private[partition] def assign(h: String, pid: PartitionId): Unit = {
        hyperedges.update(h, pid)
    }

    private[partition] def assign(v: VertexId, pid: PartitionId): Unit = {
        vertices.update(v, pid)
    }

    private def costAddHyperedge(h: String, i: PartitionId,
        extraR: Int, extraDm: Int)
    : Double = {
        val degree = HyperUtils.countDetailDegreeFromHString(h)
        val avgDg = (degrees.sum + HyperUtils.effectiveCount(degree._1, degree._2)) * 1.0 / k
        val avgR = (replicas.sum + extraR) * 1.0 / k
        val avgDm = (demands.map(_.size).sum + extraDm) * 1.0 / k

        2 * (extraR + replicas.sum) +
        costReplica * Math.pow((0 until k).filter(_ != i).map(r =>
            Math.pow(Math.abs(replicas(r) - avgR), norm)).sum +
                Math.pow(Math.abs(replicas(i) + extraR - avgR), norm),
            1.0 / norm) +
        costHyperedge * Math.pow((0 until k).filter(_ != i).map(d =>
            Math.pow(Math.abs(degrees(d) - avgDg), norm)).sum +
                Math.pow(Math.abs(degrees(i) + HyperUtils.effectiveCount(degree._1, degree._2) - avgDg), norm),
            1.0 / norm)// +
        costDemand * Math.pow((0 until k).filter(_ != i).map(d =>
            Math.pow(Math.abs(demands(d).size - avgDm), norm)).sum +
                Math.pow(Math.abs(demands(i).size + extraDm - avgDm), norm),
            1.0 / norm)
    }

    /**
     *
     * @param aux An auxiliary boolean array to indicate the following things
     *            for every vertex in h
     *            1: whether the vertex has been assigned to `to` partition
     *            2: whether the `to` partition already demands the vertex
     *            3: whether the vertex is only needed by h in `from` partition
     *            4: whether the vertex has been assigned to `from` partition
     */
    private def costMoveHyperedge(h: String, from: PartitionId,
        to: PartitionId,
        aux: OpenHashMap[VertexId, (Boolean, Boolean, Boolean, Boolean)])
    : Double = {
        if (from == to) {
            cost()
        }
        else {
            val hSet = HyperUtils.iteratorFromHString(h).toSet
            val degree = HyperUtils.countDetailDegreeFromHString(h)
            val avgDg = avg(degrees)
            val extraR = hSet.count(v => !aux(v)._1 && !aux(v)._2)
            val reducedR = hSet.count(v => aux(v)._3 && !aux(v)._4)
            val sumR = extraR - reducedR + replicas.sum
            val avgR = sumR * 1.0 / k
            val extraDm = hSet.count(v => !aux(v)._2)
            val reducedDm = hSet.count(v => aux(v)._3)
            val avgDm = (demands.map(_.size).sum + extraDm - reducedDm) * 1.0 / k
            2 * sumR +
                    costReplica * Math.pow((0 until k)
                            .filter(i => i != from && i != to).map(r =>
                        Math.pow(Math.abs(replicas(r) - avgR), norm)).sum +
                            Math.pow(Math.abs(replicas(from) - reducedR - avgR), norm) +

                            Math.pow(Math.abs(replicas(to) + extraR - avgR), norm),
                        1.0 / norm) +
                    costHyperedge * Math.pow((0 until k)
                            .filter(i => i != from && i != to).map(d =>
                        Math.pow(Math.abs(degrees(d) - avgDg), norm)).sum +
                            Math.pow(Math.abs(degrees(from) - HyperUtils.effectiveCount(degree._1, degree._2) -
                                    avgDg), norm) +
                            Math.pow(Math.abs(degrees(to) + HyperUtils.effectiveCount(degree._1, degree._2) - avgDg), norm),
                        1.0 / norm) +
                    costDemand * Math.pow((0 until k)
                            .filter(i => i!=from && i != to).map(d =>
                         Math.pow(Math.abs(demands(d).size - avgDm), norm)).sum +
                            Math.pow(Math.abs(demands(from).size - reducedDm -
                                    avgDm), norm) +
                            Math.pow(Math.abs(demands(to).size + extraDm - avgDm), norm),
                        1.0 / norm)
        }
    }


    private def costAddVertex(v: VertexId, i: PartitionId, demanded: Boolean)
    : Double = {
        val assigned = replicas.map(r => r)
        if (demanded) {
            assigned(i) = replicas(i) - 1
        }
        2 * assigned.sum + costReplica * dvt(assigned)
    }

    private def costMoveVertex(v: VertexId, from: PartitionId,
        to: PartitionId, relevantFrom: Boolean, relevantTo: Boolean): Double = {
        if (from == to) {
            2 * replicas.sum + costReplica * dvt(replicas)
        }
        else {
            val assigned = replicas.map(r => r)
            if (relevantFrom) {
                assigned(from) += 1
            }
            if (relevantTo) {
                assigned(to) -= 1
            }
            2 * assigned.sum + costReplica * dvt(assigned)
        }
    }

    private def addMaterialized(h: String, p: PartitionId): Unit = {

        val degree = HyperUtils.countDetailDegreeFromHString(h)
        degrees(p) += HyperUtils.effectiveCount(degree._1, degree._2)

        val extraReplicas = HyperUtils.iteratorFromHString(h).count(v =>
            !hasDemand(p, v) && !locals(p).contains(v))
        replicas(p) += extraReplicas

        // locals remain unchanged

        HyperUtils.iteratorFromHString(h).foreach{v =>
            addVertexToDemand(p, v)}
    }

    private def moveMaterialized(h: String, from: PartitionId,
        to: PartitionId, other: OpenHashSet[VertexId]): Unit = {

        if (from != to) {
            val degree = HyperUtils.countDetailDegreeFromHString(h)
            val effective = HyperUtils.effectiveCount(degree._1, degree._2)
            degrees(from) -= effective
            degrees(to) += effective

            val reducedReplicas = HyperUtils.iteratorFromHString(h).count(v =>
                !locals(from).contains(v) && !other.contains(v))
            val extraReplicas = HyperUtils.iteratorFromHString(h).count(v =>
                !locals(to).contains(v) && !hasDemand(to, v))
            replicas(from) -= reducedReplicas
            replicas(to) += extraReplicas

            // locals remain unchanged

            HyperUtils.iteratorFromHString(h).foreach { v =>
                removeVertexFromDemand(from, v)
                addVertexToDemand(to, v)
            }

        }

    }

    private def addMaterialized(v: VertexId, p: PartitionId): Unit = {

        // degrees remain unchanged

        val reducedReplicas = if (hasDemand(p, v)) 1 else 0
        replicas(p) -= reducedReplicas

        locals(p).add(v)

        // demands remain unchanged
    }

    private def moveMaterialized(v: VertexId, from: PartitionId,
        to: PartitionId): Unit = {

        if (from != to) {
            // degrees remain unchanged

            val reducedReplicas = if (hasDemand(to, v)) 1 else 0
            val extraReplicas = if (hasDemand(from, v)) 1 else 0
            replicas(from) += extraReplicas
            replicas(to) -= reducedReplicas

            locals(from).remove(v)
            locals(to).add(v)
        }
        // demands remain unchanged
    }

    private def otherDemand(p: PartitionId, h: String): OpenHashSet[VertexId] = {
        val other = new OpenHashSet[VertexId]()
        HyperUtils.iteratorFromHString(h).foreach{ v =>
            if (demands(p)(v) > 1) other.add(v)
        }
        other
    }


}
