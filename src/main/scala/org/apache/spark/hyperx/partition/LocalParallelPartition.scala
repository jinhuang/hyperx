package org.apache.spark.hyperx.partition

class LocalParallelPartition extends ParallelPartition {
    override private[partition] val strategy: GreedySerialPartition =
        new LocalSerialPartition()
}
