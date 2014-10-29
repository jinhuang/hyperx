package org.apache.spark.hyperx.partition

import org.apache.spark.Partitioner
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap
import org.apache.spark.hyperx.{PartitionId, VertexId}


class VertexPartitioner (val k: Int, val map: HyperXOpenHashMap[VertexId, PartitionId])
    extends Partitioner {

    override def numPartitions: Int = k

    override def getPartition(key: Any): Int = {
        key match {
            case vid: VertexId =>
                map(vid)
            case _ =>
                null.asInstanceOf[Int]
        }
    }
}
