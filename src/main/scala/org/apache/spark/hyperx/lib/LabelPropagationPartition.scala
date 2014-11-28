package org.apache.spark.hyperx.lib

import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap
import org.apache.spark.rdd.RDD

import scala.util.Random

object LabelPropagationPartition {
    def run(hypergraph: Hypergraph[_, Int], numIter: Int, numPart: PartitionId) : RDD[String] = {

        def hProg(tuple: HyperedgeTuple[Int, Int]): Iterator[(VertexId, Map[Int, Int])] = {
            val pid = tuple.attr
            (tuple.srcAttr.keySet.iterator ++ tuple.dstAttr.keySet.iterator).map(v => (v, Map(pid -> 1)))
        }

        def combine(a: Map[Int, Int], b: Map[Int, Int]): Map[Int, Int] = {
            (a.keySet ++ b.keySet).map { i =>
                val count1Val = a.getOrElse(i, 0)
                val count2Val = b.getOrElse(i, 0)
                i -> (count1Val + count2Val)
            }.toMap
        }

//        def vProg(vid: VertexId, attr: Int, message: Map[Int, Int]) = {
//            if (message == null || message.size == 0) attr else choosePid(message)
//        }

        var h = hypergraph.mapVertices((id, _) => Random.nextInt(numPart)).cache()
        val numH = h.hyperedges.map(each => each.srcIds.size + each.dstIds.size).reduce(_ + _)
        val avgH = numH / numPart
        h = h.mapTuples{tuple =>
            val candidates = calculateCandidate(tuple)
            val maxId = choosePid(candidates)
            (tuple.id, maxId)
//            val candidates = (tuple.srcAttr.iterator ++ tuple.dstAttr.iterator).toIterable.groupBy(_._2).map(p => (p._1, p._2.size))
//            val maxVal = candidates.maxBy(_._2)._2
//            val maxId = candidates.filter(_._2 == maxVal).map(i => (i, Random.nextInt())).maxBy(_._2)._1._1
//            (tuple.id, maxId)
        }

        val partitions = h.hyperedges.partitionsRDD.flatMap(part => part._2.iterator.map(h => (h.attr, h.srcIds.size + h.dstIds.size))).collect()
        val map = partitions.groupBy(i => i._1).mapValues(_.map(_._2).sum).toMap
        println("HYPERX DEBUGGING: map " + map.map(each => each._1 + " : " + each._2).reduce(_ + " ; " + _))
        val preference = new HyperXOpenHashMap[PartitionId, Double]()
        (0 until numPart).foreach(i => preference.update(i, calculatePref(avgH, map.getOrElse(i, 0))))

        var msg = h.mapReduceTuples(hProg, combine)
        var activeMsg = msg.count()

        var i = 0
        while (activeMsg > 0 && i < numIter) {

            val newVerts = h.vertices.innerJoin(msg){(vid, attr, message) =>
                if (message == null || message.size == 0) attr else choosePid(message, preference)
//               if (message == null || message.isEmpty) attr else message.maxBy(each => each._2 * preference(each._1))._1

            }.cache()
            val prevH = h

            h = h.outerJoinVertices(newVerts) {
                (vid, old,newOpt) => newOpt.getOrElse(old)
            }

            val oldMsg = msg

            h = h.mapTuples{tuple =>
                val candidates = calculateCandidate(tuple)
                val maxId = choosePid(candidates)
                (tuple.id, maxId)
            }.cache()
//            h = h.mapTuples(tuple => (tuple.id, (tuple.srcAttr.iterator ++ tuple.dstAttr.iterator).toIterable.groupBy(_._2).map(p => (p._1, p._2.size)).maxBy(_._2)._1)).cache()

            val partitions = h.hyperedges.partitionsRDD.flatMap(part => part._2.iterator.map(h => (h.attr, h.srcIds.size + h.dstIds.size))).collect()
            val map = partitions.groupBy(i => i._1).mapValues(_.map(_._2).sum).toMap
            println("HYPERX DEBUGGING: map " + map.map(each => each._1 + " : " + each._2).reduce(_ + " ; " + _))
            (0 until numPart).foreach(i => preference.update(i, calculatePref(avgH, map.getOrElse(i, 0))))

            msg = h.mapReduceTuples(hProg, combine, Some((newVerts,
                HyperedgeDirection.Both))).cache()
            activeMsg = msg.count()

            oldMsg.unpersist(blocking = false)
            newVerts.unpersist(blocking = false)
            prevH.unpersistVertices(blocking = true)
            prevH.hyperedges.unpersist(blocking = true)
            println("HYPERX DEBUGGING: preference in " + i + " " + preference.map(each => each._1 + " " + each._2).reduce(_ + " ; " + _))
            i += 1
        }

        println("HYPERX DEBUGGING: preference " + preference.map(each => each._1 + " " + each._2).reduce(_ + " ; " + _))

        h.hyperedges.partitionsRDD.flatMap[String]{part =>
            part._2.tupleIterator(true, true).map{tuple =>
                tuple.attr + " : " + tuple.srcAttr.map(_._1.toString()).reduce(_ + " " + _) + " : " + tuple.dstAttr.map(_._1.toString()).reduce(_+ " " + _)
            }
        }
    }

    private def calculateCandidate(tuple: HyperedgeTuple[Int, Int]): Map[Int, Int] = {
        (tuple.srcAttr.iterator ++ tuple.dstAttr.iterator).toIterable.groupBy(_._2).map(p => (p._1, p._2.size))
    }

//    private def calculateCandidate(tuple: HyperedgeTuple[Int, Int], preference: HyperXOpenHashMap[PartitionId, Double]): Map[Int, Int] = {
//        (tuple.srcAttr.iterator ++ tuple.dstAttr.iterator).toIterable.groupBy(_._2).map(p => (p._1, (p._2.size * preference(p._1)).toInt))
//    }

    private def choosePid(map: Map[Int, Int]): PartitionId = {
//        val sorted = map.toArray.sortBy(_._2)
//        val size = sorted.size
//        val sum = sorted.map(_._2).sum
//        val cumulative = (0 until size).map(i => (sorted(i)._1, (0 to i).map(j => sorted(j)._2).sum.toDouble / sum)).toArray
//        val randomDouble = Random.nextDouble()
//        var index = 0
//        if (size > 1 && cumulative(0)._2 < randomDouble) {
//            index = (1 until size).filter(i => cumulative(i)._2 > randomDouble && cumulative(i - 1)._2 <= randomDouble)(0)
//        }
//        cumulative(index)._1
        val maxVal = map.maxBy(_._2)._2
        map.filter(_._2 == maxVal).map(i => (i, Random.nextInt())).maxBy(_._2)._1._1
    }

    private def choosePid(map: Map[Int, Int], preference: HyperXOpenHashMap[PartitionId, Double]): PartitionId = {
//        val sorted = map.map(each => (each._1, (preference(each._1) * each._2).toInt)).toArray.sortBy(_._2)
//        val size = sorted.size
//        val sum = sorted.map(_._2).sum
//        val cumulative = (0 until size).map(i => (sorted(i)._1, (0 to i).map(j => sorted(j)._2).sum.toDouble / sum)).toArray
//        val randomDouble = Random.nextDouble()
//        var index = 0
//        if (size > 1 && cumulative(0)._2 < randomDouble) {
//            index = (1 until size).filter(i => cumulative(i)._2 > randomDouble && cumulative(i - 1)._2 <= randomDouble)(0)
//        }
//        cumulative(index)._1
        map.maxBy(each => each._2 * preference(each._1))._1
    }

    private def calculatePref(avg: Int, curr: Int) : Double = {
        Math.pow(Math.E, (Math.pow(avg, 2) - Math.pow(curr, 2))/ Math.pow(avg, 2))
    }
}
