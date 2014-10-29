package org.apache.spark.hyperx.partition

import org.apache.spark.hyperx.util.HyperUtils

class OnePassSerialPartition extends GreedySerialPartition {
    override private[partition] def search() = {
        var start = System.currentTimeMillis()
        randomVertices()
        logInfo("HYPERX DEBUGGING: P2.0 search.randomVertices in %d ms".format(System.currentTimeMillis() - start))
        start = System.currentTimeMillis()
        materialize()
        logInfo("HYPERX DEBUGGING: P2.1 search.materialize in %d ms".format(System.currentTimeMillis() - start))
        start = System.currentTimeMillis()
        hyperedges.foreach{h =>
            val pid = addHyperedge(h._1)
            HyperUtils.iteratorFromHString(h._1).foreach{v =>
                assign(v, pid)
            }
        }
        logInfo("HYPERX DEBUGGING: P2.2 search.assignHyperedges in %d ms".format(System.currentTimeMillis() - start))
        start = System.currentTimeMillis()
        vertices.foreach(v => vertices.update(v._1, moveVertex(v._1, v._2)))
        logInfo("HYPERX DEBUGGING: P2.3 search.assignVertices in %d ms".format(System.currentTimeMillis() - start))
    }
}
