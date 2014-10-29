package org.apache.spark.hyperx

/**
 * Collection of partitioning heuristics
 */
package object partition {
    private[partition] var costHyperedge = 5.0
    private[partition] var costReplica = 10.0
    private[partition] var costDemand = 1.0
    private[partition] var norm = 2

    private[partition] val searchEpsilonFraction = 0.01
}
