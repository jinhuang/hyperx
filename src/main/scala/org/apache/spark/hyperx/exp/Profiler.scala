package org.apache.spark.hyperx.exp

import java.io.{BufferedReader, FileReader}

import org.apache.spark.hyperx.VertexId
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap

object Profiler {

    type HyperedgeId = Int

    def main(args: Array[String]): Unit = {

//        val size = 662998
//
//        val map = new HyperXOpenHashMap[VertexId, new HyperXOpenHashMap[]()]()

//        val file = Source.fromFile("/run/media/soone/Data/hyperx/result/statistics/part-00001")
//        val ret = file.getLines().map{line =>
//            val array = line.replace("(", "").replace(")", "").split(",")
//            val id = array(0).toLong
//            val size = array(1).toInt
//            (id, Array.fill(size)(0L), Array.fill(size)(0.0))
//        }
//        println(ret.map(i => i._2.size).sum)
//
//        val stop = readLine()
//        println(ret.map(i => 1).reduce(_ + _))
        val files = new java.io.File("/run/media/soone/Data/hyperx/result/statistics").listFiles()

        val map = new HyperXOpenHashMap[VertexId, HyperXOpenHashMap[VertexId, Double]]()
        files.foreach{ file =>
            val reader = new BufferedReader(new FileReader(file))
            var line = reader.readLine()

            while(line != null) {
                val array = line.replace("(", "").replace(")", "").split(",")
                val id = array(0).toLong
                val size = array(1).toInt
                map.update(id, new HyperXOpenHashMap[VertexId, Double]())
                val thisMap = map(id)
                (0 until size).foreach{i =>
                    thisMap.update(i.toLong, i.toDouble)
                }
                line = reader.readLine()
            }
        }

        val stop = readLine()
        println(map.map(i => 1).reduce(_ + _))
//        println(allPairs.map(i => 1).reduce(_ + _))

        // comparing two maps with map of a two-tuple
//        val mapA = new HyperXOpenHashMap[Int, Int]()
//        val mapB = new HyperXOpenHashMap[Int, Int]()
//        val set = new HyperXOpenHashSet[Long]()
//
//        (0 until 1115729).foreach{i =>
////            mapA.update(i, i + 1)
////            mapB.update(i, i + 1)
//            set.add(Random.nextInt(2322299))
//        }
////        val ret = mapA.map(i => i._2 - i._1).reduce(_ + _) + mapB.map(i => i._2 - i._1).reduce(_ + _)
////        val ret = map.map(i => i._2._2 - i._2._1).reduce(_ + _)
//        val ret = set.iterator.map(i => 1).reduce(_ + _)
//        val inputFile = "/run/media/soone/Data/hyperx/datasets/exp/hyperx/orkut_communities"
//        val source = Source.fromFile(inputFile)
//        val lines = source.getLines()
//        var builder = new FlatHyperedgePartitionBuilder[HyperAttr[Double], Double]()
//        lines.filterNot(s => s.isEmpty || s.startsWith("#") || s.split(":").length < 2).zipWithIndex.filter(i => Random.nextInt(28) == 0).foreach{line =>
//            val ary = line._1.split(":").map(_.trim).filter(_.nonEmpty)
//            val src = new VertexSet()
//            ary(0).split(" ").map(_.trim).foreach(v => src.add(v.toLong))
//            val dst = new VertexSet()
//            ary(1).split(" ").map(_.trim).foreach(v => dst.add(v.toLong))
//            val attr = new HyperXOpenHashMap[VertexId, Double]()
//            src.iterator.foreach(v => attr.update(v, 0.0))
//            builder.add(src, dst, line._2, attr)
//        }
//
//        val partition = builder.toFlatHyperedgePartition
//        val vertices = partition.vertexIds.toSet[VertexId].iterator.map(v => (v, 0.0))
//        val vertexIds = new VertexSet
//        vertices.foreach(v => vertexIds.add(v._1))
//        val vertexPartition = VertexPartition(vertices)
//        partition.withVertices[Double](vertexPartition)
//        partition.withActiveSet(Some(vertexIds))
////        vertexPartition = null
//        builder = null
//        val newVertices = vertices.map(v => (v._1, 1.0))
//        partition.updateVertices(newVertices)
//        val stop = readLine()
//        println(partition.iterator.size)
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
