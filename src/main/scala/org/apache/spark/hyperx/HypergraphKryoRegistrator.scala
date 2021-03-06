package org.apache.spark.hyperx

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.hyperx.impl._
import org.apache.spark.hyperx.partition._
import org.apache.spark.hyperx.util.collection.{HyperXOpenHashMap, HyperXOpenHashSet}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet}

import scala.collection.{BitSet, immutable, mutable}

/**
 * Register HyperX classes with Kryo
 */
class HypergraphKryoRegistrator extends KryoRegistrator {
    def registerClasses(kryo: Kryo) = {

        kryo.register(classOf[(VertexId, Object)])
        kryo.register(classOf[BitSet])
        kryo.register(classOf[VertexIdToIndexMap])
        kryo.register(classOf[VertexAttributeBlock[Object]])
        kryo.register(classOf[PartitionStrategy])
        kryo.register(classOf[BoundedPriorityQueue[Object]])
        kryo.register(classOf[HyperXOpenHashSet[Object]])
        kryo.register(classOf[HyperXOpenHashMap[Object, Object]])
        kryo.register(classOf[OpenHashSet[Object]])
        kryo.register(classOf[OpenHashMap[Object, Object]])
        kryo.register(classOf[mutable.HashMap[Object, Object]])
        kryo.register(classOf[immutable.Map[Object, Object]])
        kryo.register(classOf[Array[Object]])
        kryo.register(classOf[Array[Int]])
        kryo.register(classOf[Array[Long]])
        kryo.register(classOf[Array[Double]])
        kryo.register(classOf[(Any, Any, Any)])
        kryo.register(classOf[HyperedgePartition[Object, Object]])
        kryo.register(classOf[FlatHyperedgePartition[Object, Object]])
        kryo.register(classOf[Hyperedge[Object]])
        kryo.register(classOf[HyperedgeDirection])
        kryo.register(classOf[ReplicatedVertexView[Object, Object]])
        kryo.register(classOf[PlainPartition])
        kryo.register(classOf[GreedyPartition])
        kryo.register(classOf[BipartitePartition])
        kryo.register(classOf[AwetoPartition])
        kryo.register(classOf[ReplicaPartition])
    }
}
