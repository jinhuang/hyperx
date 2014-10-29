package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap
import org.apache.spark.hyperx.{VertexId, HyperAttr, HyperedgeTuple}

object Profiler {
    def main(args: Array[String]): Unit = {
        val h = new HyperedgeTuple[Double, HyperAttr[Double]]()
        h.srcAttr = new HyperAttr()
        (0 until 10000).foreach{i => h.srcAttr.update(i,i)}
        h.dstAttr = new HyperAttr()
        (0 until 0).foreach{i => h.dstAttr.update(i,i)}
        h.attr = new HyperXOpenHashMap[VertexId, Double]()
        h.srcAttr.foreach{v => h.attr.update(v._1, v._2)}

        val start = System.currentTimeMillis()
        var src: Long = 0
        var dst: Long = 0
        (0 until 10000).foreach{i =>
            var msg = 0.0
            val srcStart = System.currentTimeMillis()
            msg = h.srcAttr.map{j => h.attr(j._1)}.sum / h.dstAttr.size
            src += System.currentTimeMillis() - srcStart
            val dstStart = System.currentTimeMillis()
            val result = h.dstAttr.map(j => (j._1, msg))
            result.size
            dst+= System.currentTimeMillis() - dstStart
        }
        println(System.currentTimeMillis() - start + " " + src + " " + dst)
    }
}
