package org.apache.spark.hyperx

import org.apache.spark.hyperx.util.collection.HyperXOpenHashSet

/**
 * Collection of implementation classes of HyperX
 *
 * Forked from GraphX 2.10, modified by Jin Huang
 */
package object impl {
    private[hyperx] type VertexIdToIndexMap = HyperXOpenHashSet[VertexId]
}
