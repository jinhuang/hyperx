package org.apache.spark.hyperx

import org.apache.spark.hyperx.impl.{FlatHyperedgePartition, HypergraphImpl, ShippableVertexPartition}
import org.apache.spark.hyperx.partition.{PartitionStrategy, PlainPartition}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}

/**
 * Provided utilities for loading [[Hypergraph]]s from files.
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */
object HypergraphLoader extends Logging {

    /**
     * Load a hypergraph from a hyperedge list formatted file where each line
     * contains a hyperedge as:
     * srcIds separator dstIds separator weight. Skip lines that begin with `#`.
     *
     * Optionally, hyperedge weight can be skipped and set to 1 by default.
     */
    def hyperedgeListFile(
         sc: SparkContext, path: String, separator: String,
         weighted: Boolean = false, part: PartitionId,
         strategy: PartitionStrategy = new PlainPartition(),
         hyperedgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
         vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Hypergraph[Int, Int] = {
        val lines = sc.textFile(path, part)
        val input = lines.filter{ line =>
            val array = line.split(separator)
            !line.isEmpty &&
            line(0) != '#' &&
            line.contains(separator) &&
            array.length >= 2
            }
            .map{line =>
                val lineArray = line.replace(separator, ";").split(";")
                lineArray(0).trim + ";" + lineArray(1).trim // strip the hyperedge value for now
            }

        HypergraphImpl.fromHyperedgeList[Int, Int](
            input, part, strategy, vertexStorageLevel, hyperedgeStorageLevel)

    }

    /**
     * Load a hypergraph from stored object files
     */
    def hypergraphObjectFile( sc: SparkContext, vertexPath: String,
        hyperedgePath: String, part: PartitionId,
        hyperedgeLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        vertexLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Hypergraph[Int,Int]= {
        val hyperedges =
            sc.objectFile[(PartitionId, FlatHyperedgePartition[Int, Int])](
                hyperedgePath, part)
        val vertices =
            sc.objectFile[ShippableVertexPartition[Int]](vertexPath, part)

        HypergraphImpl.fromPartitions[Int, Int](
            hyperedges, vertices,hyperedgeLevel, vertexLevel)
    }

    /**
     * Load a hypergraph from a partitioned hyperedge list formatted file 
     * where each line contains a hyperedge as:
     * partitionId separator srcIds separator dstIds separator weight 
     * Skip lines that begin with `#`.
     *
     * Optionally, hyperedge weight can be skipped and set to 1 by default.
     */
    def partitionFile(sc: SparkContext, path: String, part: PartitionId,
        separator: String,
        hyperedgeLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        vertexLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Hypergraph[Int, Int] = {
        val lines = sc.textFile(path, part)
        val input = lines.filter{ line =>
            val array = line.split(separator)
            !line.isEmpty && line(0) != '#' && line.contains(separator) &&
                array.length >= 3
        }.map(line => line.replace(separator, ";"))

        HypergraphImpl.fromPartitionedHyperedgeList[Int, Int](
            input, part, vertexLevel, hyperedgeLevel)
    }
}
