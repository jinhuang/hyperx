package org.apache.spark.hyperx.exp

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, Vector => BV}
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap

object Profiler {


    def main(args: Array[String]): Unit = {
        val map = new HyperXOpenHashMap[Int, Int]()
        (0 until 10).foreach(i  => map.update(i, i *2))

        println(map.iterator.map(i => i._1 + " " + i._2).reduce(_ + " ; " + _))
    }

}
