package org.apache.spark.hyperx.partition.obsolete

import org.apache.spark.hyperx.partition.obsolete.ParallelPartition

class AlternateParallelPartition extends ParallelPartition {
    override private[partition] val strategy: GreedySerialPartition =
        new AlternateSerialPartition()
}
