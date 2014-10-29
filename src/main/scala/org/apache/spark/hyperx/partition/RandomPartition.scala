package org.apache.spark.hyperx.partition

class RandomPartition extends SerialPartition {
    override private[partition] def search(): Unit = {
        randomHyperedges()
        randomVertices()
        materialize()
    }
}
