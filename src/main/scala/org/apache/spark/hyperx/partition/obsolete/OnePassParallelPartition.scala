package org.apache.spark.hyperx.partition.obsolete

import org.apache.spark.hyperx.partition.obsolete.ParallelPartition

class OnePassParallelPartition extends ParallelPartition {
    override private[partition] val strategy: GreedySerialPartition =
        new OnePassSerialPartition()
}
