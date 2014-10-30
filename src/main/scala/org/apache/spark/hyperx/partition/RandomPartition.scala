package org.apache.spark.hyperx.partition

import scala.util.Random

class RandomPartition extends SearchPartition {
    override private[partition] def search(): Unit = {
        randomHyperedges()
        randomVertices()
        materialize()
    }

    private[partition] def randomHyperedges() = {
        hyperedges.keySet.iterator.foreach{key =>
            hyperedges.update(key, Random.nextInt(k))
        }
    }

    private[partition] def randomVertices() = {
        vertices.keySet.iterator.foreach{v =>
            vertices.update(v, Random.nextInt(k))}
    }
}
