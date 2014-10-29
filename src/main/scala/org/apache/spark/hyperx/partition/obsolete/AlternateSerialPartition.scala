package org.apache.spark.hyperx.partition.obsolete

class AlternateSerialPartition extends GreedySerialPartition {

    override private[partition] def search() = {
        randomHyperedges()
        randomVertices()
        materialize()
        var minimized = false
        var i = 0
        while(!minimized && i < MAX_ITER) {
            val reduced = iterate()
            minimized = (reduced * 1.0 / cost()) < REDUCED_RATIO
            i += 1
        }
    }

    private def iterate(): Double = {
        val prevCost = cost()
        optimizeHyperedge()
        optimizeVertex()
        val afterCost = cost()
//        assert((prevCost - afterCost) >= 0.0)
        logInfo("HYPERX: Alternate cost " + afterCost)
        prevCost - afterCost
    }

    private def optimizeHyperedge() = {
        hyperedges.foreach{h =>
            moveHyperedge(h)
        }
    }

    private def optimizeVertex() = {
        vertices.foreach{v =>
            moveVertex(v)
        }
    }

    private val REDUCED_RATIO = 0.1

    private val MAX_ITER = 5
}
