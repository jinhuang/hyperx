package org.apache.spark.hyperx.partition

class OnePassParallelPartition extends ParallelPartition {
    override private[partition] val strategy: GreedySerialPartition =
        new OnePassSerialPartition()
}
