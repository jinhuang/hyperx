package org.apache.spark.hyperx.exp

import org.apache.spark.hyperx.VertexId
import org.apache.spark.hyperx.util.collection.{HyperXOpenHashMap,
HyperXPrimitiveVector}

import scala.io.Source

object Profiler {

    type HyperedgeId = Int

    def main(args: Array[String]): Unit = {
        val inputFile = "/run/media/soone/Data/hyperx/datasets/exp/hyperx/orkut_communities"
        val source = Source.fromFile(inputFile)
        val lines = source.getLines()

        val dataCollected = (new HyperXPrimitiveVector[HyperedgeId](),
                new HyperXPrimitiveVector[VertexId](),
                new HyperXPrimitiveVector[Boolean](),
                new HyperXOpenHashMap[HyperedgeId, Int]())

//        val dataCollected = (new HyperXOpenHashSet[VertexId](), new HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[HyperedgeId]](),
//                new HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[Boolean]]())

        lines.filterNot(s => s.isEmpty || s.startsWith("#") || s.split(":").length < 2)
             .zipWithIndex.foreach{s =>
            val ary = s._1.split(":").map(_.trim).filter(_.nonEmpty)
//            val hS = ary(0) + ";" + ary(1)
//            val pair = HyperUtils.hyperedgeFromHString(hS)
//            Iterator(pair._1, pair._2)
            ary(0).split(' ').map(v => (v.toLong, s._2, true)) ++ ary(1).split(' ').map(v => (v.toLong, s._2, false))
            dataCollected._4.update(s._2, dataCollected._1.size)
            ary(0).split(' ').foreach{v =>
                dataCollected._2 += v.toLong
                dataCollected._1 += s._2
                dataCollected._3 += true
            }
            ary(1).split(' ').foreach{v =>
                dataCollected._2 += v.toLong
                dataCollected._1 += s._2
                dataCollected._3 += false
            }
//            ary(0).split(' ').foreach{v =>
//                val id = v.toLong
//                if (!dataCollected._1.contains(id)) {
//                    dataCollected._1.add(id)
//                    dataCollected._2.update(id, new HyperXPrimitiveVector[HyperedgeId]())
//                    dataCollected._3.update(id, new HyperXPrimitiveVector[Boolean]())
//                }
//                dataCollected._2(id) += s._2
//                dataCollected._3(id) += true
//            }
//            ary(1).split(' ').foreach{v =>
//                val id = v.toLong
//                if (!dataCollected._1.contains(id)) {
//                    dataCollected._1.add(id)
//                    dataCollected._2.update(id, new HyperXPrimitiveVector[HyperedgeId]())
//                    dataCollected._3.update(id, new HyperXPrimitiveVector[Boolean]())
//                }
//                dataCollected._2(id) += s._2
//                dataCollected._3(id) += false
//            }
        }

//        val idCollected = dataCollected._1.iterator.toArray
//        var hCollected = new HyperXPrimitiveVector[Array[HyperedgeId]]()
//        var fCollected = new HyperXPrimitiveVector[Array[Boolean]]()
//
//        idCollected.foreach{v =>
//            hCollected += dataCollected._2(v).array
//            fCollected += dataCollected._3(v).array
//        }
//
//
//
//        val aryCollected = (idCollected, hCollected.trim().array, fCollected.trim().array)
//        hCollected = null
//        fCollected = null
//        println("data " + aryCollected._1.size)

        val arrayCollected = (dataCollected._1.trim().array, dataCollected._2.trim().array, dataCollected._3.trim().array, dataCollected._4)

        println("data " + arrayCollected._1.size + " " + arrayCollected._4.size)

        // the current implementation
//        val srcAry = data.map(_._1).toIterator.toArray
//        val dstAry = data.map(_._2).toIterator.toArray
//
//        println("src " + srcAry.size + " dst " + dstAry.size)

        // tuple
//        val collection = data.zipWithIndex
//                .flatMap(h => h._1._1.iterator.map(v => (v, h._2, true)) ++ h._1._2.iterator.map(v => (v, h._2, false)))
//                .groupBy(_._1).mapValues(_.map(_._2)).iterator.toArray
//        println("collection " + collection.size)

        val stop = readLine()
        source.close()
    }
}
