package org.apache.spark.hyperx.partition

class AlternateParallelPartition extends ParallelPartition {
    override private[partition] val strategy: GreedySerialPartition =
        new AlternateSerialPartition()
}
