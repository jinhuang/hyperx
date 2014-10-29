package org.apache.spark.hyperx.impl

import org.apache.spark.hyperx.util.collection.{HyperXOpenHashMap, HyperXPrimitiveVector}
import org.apache.spark.hyperx.{Hyperedge, VertexId, VertexSet}

import scala.reflect.ClassTag

/** Forked from GraphX 2.10, modified by Jin Huang */
private[hyperx]
class HyperedgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassTag,
VD: ClassTag](size: Int = 64, withIndex: Boolean = true) extends
    Serializable {
    var hyperedges = new HyperXPrimitiveVector[Hyperedge[ED]](size)
    var indices = new HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[Int]]
    var dstIndices =
        new HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[Int]]()

    /** Add a new hyperedge to the partition. */
    def add(src: VertexSet, dst: VertexSet, d: ED): Unit = {
        hyperedges += Hyperedge(src, dst, d)
    }

    def toHyperedgePartition: HyperedgePartition[ED, VD] = {
        val hyperedgeArray = hyperedges.trim().array
        val srcIds = new Array[VertexSet](hyperedgeArray.size)
        val dstIds = new Array[VertexSet](hyperedgeArray.size)
        val data = new Array[ED](hyperedgeArray.size)

        if (hyperedgeArray.length > 0) {
            var i = 0
            while (i < hyperedges.size) {
                srcIds(i) = hyperedgeArray(i).srcIds
                dstIds(i) = hyperedgeArray(i).dstIds
                data(i) = hyperedgeArray(i).attr
                encounterVertices(srcIds(i), i)
                encounterDstVertices(dstIds(i), i)
                i += 1
            }
        }

        val builtIndex = if (!withIndex) null else makeIndex
        // populate the vertex partition
        val vertexIds = indices.keySet
        dstIndices.keySet.iterator.foreach(vertexIds.add)

        val vertices = new VertexPartition(
            vertexIds, new Array[VD](vertexIds.capacity), vertexIds.getBitSet)

        val partition =
            new HyperedgePartition(srcIds, dstIds, data, builtIndex, vertices)
        partition
    }

    private def encounterVertices(vids: VertexSet, i: Int) = {
        val it = vids.iterator
        while (it.hasNext) {
            val curId = it.next()
            if (indices.apply(curId) == null) {
                if (withIndex)
                    indices.update(curId, new HyperXPrimitiveVector[Int]())
                else
                    indices.update(curId, null)
            }
            if (withIndex)
                indices.apply(curId) += i
        }
    }

    private def encounterDstVertices(vids: VertexSet, i: Int) = {
        val it = vids.iterator
        while(it.hasNext) {
            val curId = it.next()
            dstIndices.update(curId, null)
        }
    }


    private def makeIndex: HyperXOpenHashMap[VertexId, Array[Int]] = {
        val it = indices.iterator
        val map = new HyperXOpenHashMap[VertexId, Array[Int]]()
        while (it.hasNext) {
            val cur = it.next()
            map.update(cur._1, cur._2.trim().array)
        }
        map
    }
}
