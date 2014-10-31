package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.VertexId
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.SparkContext._

import scala.util.Random


class GreedyPartition extends PartitionStrategy{
    override private[partition] def search(input: RDD[String]): Unit = {
        hRDD = input.coalesce(k / parallelism, shuffle = true)
                .mapPartitionsWithIndex({(i, p) =>
            val demands = Array.fill(parallelism)(new OpenHashSet[VertexId]())
            val degrees = Array.fill(parallelism)(0)
            val sorted = p.toArray.sortBy(h => 0 - HyperUtils
                    .countDegreeFromHString(h))
            sorted.map{h =>
//                val pid = (0 until parallelism).map(i =>
//                    (i, demands(i).size * costDemand + degrees(i) * costHyperedge)).minBy(_._2)._1
//                HyperUtils.iteratorFromHString(h).foreach(demands(pid).add)
//                degrees(pid) += HyperUtils.countDegreeFromHString(h)
//                (h, pid + parallelism * i)
                (h, Random.nextInt(parallelism) + parallelism * i)
            }.iterator
        })

        val demands = hRDD.map(h => Tuple2(h._2, HyperUtils.iteratorFromHString(h._1).toSet)).reduceByKey(_.union(_)).collect()
        val broadcastDemands = hRDD.context.broadcast(demands)

        vRDD = hRDD.flatMap(h => HyperUtils.iteratorFromHString(h._1)).distinct(k / parallelism)
                .mapPartitionsWithIndex{(i, p) =>
            val locals = Array.fill(parallelism)(0)
            p.map{v =>
//                val pid = (0 until parallelism).map(i =>
//                    (i, (if (broadcastDemands.value(i)._2.contains(v)) 1 else 0) * costReplica - locals(i))).maxBy(_._2)._1
//                locals(pid) += 1
//                (v, pid + parallelism * i)
                (v, Random.nextInt(parallelism) + parallelism * i)
            }.toIterator
        }
    }

    private val parallelism = 1
}
