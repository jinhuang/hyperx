package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.VertexId

import scala.collection.mutable

object Profiler {
    def main(args: Array[String]): Unit = {
        val openMap = new mutable.OpenHashMap[VertexId, Int]()

        (0 until 10000).foreach{i =>
            openMap.update(i,i)
        }
        val start = System.currentTimeMillis()
        (0 until 10000).foreach{i =>
            openMap.size
        }
        println(System.currentTimeMillis() - start)
    }
}
