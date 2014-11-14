package org.apache.spark.hyperx.impl

import org.apache.spark.hyperx.{VertexSet, VertexId, HyperedgeId, Hyperedge}
import org.apache.spark.hyperx.util.collection.{HyperXOpenHashMap,
HyperXPrimitiveVector}

import scala.reflect.ClassTag

//private[hyperx]
class FlatHyperedgePartitionBuilder[
    @specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    size: Int = 64, withIndex: Boolean = true) extends Serializable {

    var hyperedges = new HyperXPrimitiveVector[Hyperedge[ED]](size)
    var hyperedgeIds = new HyperXPrimitiveVector[HyperedgeId](size)
    var hIndices = new HyperXOpenHashMap[HyperedgeId, Int]()
    var srcIndices =
        new HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[HyperedgeId]]()
    var dstIndices =
        new HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[HyperedgeId]]()

    def add(src: VertexSet, dst: VertexSet, hid: HyperedgeId, data: ED)
    : Unit = {
        hyperedges += Hyperedge(src, dst, data)
        hyperedgeIds += hid
    }

    def toFlatHyperedgePartition: FlatHyperedgePartition[ED, VD] = {
        val hyperedgeArray = hyperedges.trim().array
        val hyperedgeIdArray = hyperedgeIds.trim().array
        val vertexIds = new HyperXPrimitiveVector[VertexId]()
        val hIds = new HyperXPrimitiveVector[HyperedgeId]()
        val srcFlags = new HyperXPrimitiveVector[Boolean]()
        val data = new HyperXOpenHashMap[HyperedgeId, ED]()
        val allVertices = new VertexSet()

        if (hyperedgeArray.size > 0) {
            var i = 0
            while(i < hyperedgeArray.size) {
                val h = hyperedgeArray(i)
                val hId = hyperedgeIdArray(i)
                val pos = vertexIds.size
                hIndices.update(hId, pos)
                h.srcIds.iterator.foreach{v =>
                    vertexIds += v
                    hIds += hId
                    srcFlags += true
                    updateIndex(srcIndices, v, hId)
                    allVertices.add(v)
                }
                h.dstIds.iterator.foreach{v =>
                    vertexIds += v
                    hIds += hId
                    srcFlags += false
                    updateIndex(dstIndices, v, hId)
                    allVertices.add(v)
                }
                data(hId) = hyperedgeArray(i).attr
                i += 1
            }
        }

        val builtIndex = if (!withIndex) null else makeIndex

        // this was a notorious bug that sucked more than 2 hours to hunt down,
        // the mistake is using allVertices.size instead of allVertices.capacity,
        // which leads the values in the vertex partition to be of length much
        // smaller than it should be!
        val vertices = new VertexPartition(
            allVertices, new Array[VD](allVertices.capacity), allVertices.getBitSet
        )

        new FlatHyperedgePartition(vertexIds.trim().array,
            hIds.trim().array, srcFlags.trim().array, data,
            builtIndex, hIndices, vertices)

    }

    private def updateIndex(
        index: HyperXOpenHashMap[VertexId, HyperXPrimitiveVector[HyperedgeId]],
        vid: VertexId, hId: HyperedgeId) = {
        if (!index.hasKey(vid)) {
            index.update(vid, new HyperXPrimitiveVector[HyperedgeId]())
        }
        index(vid) += hId
    }

    private def makeIndex = {
        val map = new HyperXOpenHashMap[VertexId, Array[HyperedgeId]]()
        srcIndices.foreach(m => map.update(m._1, m._2.trim().array))
        map
    }
}
