package org.apache.spark.hyperx.exp

import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap

object Profiler {

    type HyperedgeId = Int

    def main(args: Array[String]): Unit = {


        // comparing two maps with map of a two-tuple
//        val mapA = new HyperXOpenHashMap[Int, Int]()
//        val mapB = new HyperXOpenHashMap[Int, Int]()
        val map = new HyperXOpenHashMap[Int, (Int, Int)]()

        (0 until 10000000).foreach{i =>
//            mapA.update(i, i + 1)
//            mapB.update(i, i + 1)
            map.update(i, (i + 1, i + 2))
        }

//        val ret = mapA.map(i => i._2 - i._1).reduce(_ + _) + mapB.map(i => i._2 - i._1).reduce(_ + _)
        val ret = map.map(i => i._2._2 - i._2._1).reduce(_ + _)
        println(ret)
        val stop = readLine()
        // comparing arrays and hashMap
//        val map = new HyperXOpenHashMap[VertexId, Int]()
//        val keyVec = new HyperXPrimitiveVector[VertexId]()
//        val valVec = new HyperXPrimitiveVector[Int]()
//        (0 until 10000000).foreach{i =>
//            keyVec += i.toLong
//            valVec += i
//        }
////        val ret = map.map(i => i._2).reduce(_ + _)
//        val keyAry = keyVec.trim().array
//        val valAry = valVec.trim().array
//        println(keyAry.size + " " + valAry.size)
//        val stop = readLine()
        // comparing arrays and sets
//        val inputFile = "/run/media/soone/Data/hyperx/datasets/exp/hyperx/lastfm_similars"
//        val source = Source.fromFile(inputFile)
//        val lines = source.getLines()
//
//        val data = lines.filterNot(s => s.isEmpty || s.startsWith("#") || s.split(":").length < 2).map{line =>
//            val ary = line.split(":").map(_.trim).filter(_.nonEmpty)
////            val src = new HyperXPrimitiveVector[VertexId]()
//            val src = new VertexSet()
//            ary(0).split(" ").map(_.trim).foreach(v => src.add(v.toLong))
//            val dst = new VertexSet()
//            ary(1).split(" ").map(_.trim).foreach(v => dst.add(v.toLong))
//            (src, dst)
////            ary(0).split(" ").map(_.trim).foreach(v => src += v.toLong)
////            val dst = new HyperXPrimitiveVector[VertexId]()
////            ary(1).split(" ").map(_.trim).foreach(v => dst += v.toLong)
////            (src.trim().array, dst.trim().array)
//        }.toIndexedSeq
//
//        val src = data.map(_._1)
//        val dst = data.map(_._2)
//
//        println("data length: " + src.size + " " + dst.size)
//
//        val stop = readLine()
//        println("data length: " + data.size)
//        source.close()
//        val map = new HyperXOpenHashMap[HyperedgeId, Int]()
//        (0 until 10).foreach(i => map.update(i * 2,i))
//        val start = System.currentTimeMillis()
//        var pos = 0
//        (0 until 10).foreach{i =>
//            val nextPos = map.nextPos(pos)
//            pos = nextPos + 1
//            println(map._values(nextPos))
//        }
//        (0 until 1000000).foreach {i =>
//            val tmp = new VertexSet
//            (0 until 100).foreach{j => tmp.add(j.toLong)}
//        }

//        (0 until 10000).foreach{i =>
//            val test = map.nth(i)
//        }
//        println(System.currentTimeMillis() - start)


//
//        val dataCollected = (new HyperXPrimitiveVector[HyperedgeId](),
//                new HyperXPrimitiveVector[VertexId](),
//                new HyperXPrimitiveVector[Boolean](),
//                new HyperXOpenHashMap[HyperedgeId, Int]())
//
////        val dataCollected = (new HyperXOpenHashSet[VertexId](), new HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[HyperedgeId]](),
////                new HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[Boolean]]())
//
//        lines.filterNot(s => s.isEmpty || s.startsWith("#") || s.split(":").length < 2)
//             .zipWithIndex.foreach{s =>
//            val ary = s._1.split(":").map(_.trim).filter(_.nonEmpty)
////            val hS = ary(0) + ";" + ary(1)
////            val pair = HyperUtils.hyperedgeFromHString(hS)
////            Iterator(pair._1, pair._2)
//            ary(0).split(' ').map(v => (v.toLong, s._2, true)) ++ ary(1).split(' ').map(v => (v.toLong, s._2, false))
//            dataCollected._4.update(s._2, dataCollected._1.size)
//            ary(0).split(' ').foreach{v =>
//                dataCollected._2 += v.toLong
//                dataCollected._1 += s._2
//                dataCollected._3 += true
//            }
//            ary(1).split(' ').foreach{v =>
//                dataCollected._2 += v.toLong
//                dataCollected._1 += s._2
//                dataCollected._3 += false
//            }
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
//        }

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

//        val arrayCollected = (dataCollected._1.trim().array, dataCollected._2.trim().array, dataCollected._3.trim().array, dataCollected._4)
//
//        println("data " + arrayCollected._1.size + " " + arrayCollected._4.size)

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

//        val stop = readLine()
//        source.close()
    }
}
