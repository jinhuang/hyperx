package org.apache.spark.hyperx.partition

import org.apache.spark.Logging
import org.apache.spark.hyperx._
import org.apache.spark.rdd.RDD

/**
 * Represents the way hyperedges and vertices are assigned to partitions.
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */
trait PartitionStrategy extends Serializable with Logging {

    def partition(numParts: PartitionId, input: RDD[String])
    : (RDD[(PartitionId, VertexId)], RDD[(PartitionId, String)])



    /** Returns the partition number for a given vertex based on its id. */
//    def getPartition(vid: VertexId): PartitionId

    /** Returns the partition number for a given hyperedge based on its
      * vertex sets. */
//    def getPartition(srcIds: VertexSet, dstIds: VertexSet): PartitionId
}