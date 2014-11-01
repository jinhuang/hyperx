package org.apache.spark.hyperx.partition

object Profiler {
    def main(args: Array[String]): Unit = {
        val start = System.currentTimeMillis()
        (0 until 1000000).map{i => i}.toIndexedSeq
        println(System.currentTimeMillis() - start)
    }
}
