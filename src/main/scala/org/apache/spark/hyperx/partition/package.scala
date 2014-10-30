package org.apache.spark.hyperx

/**
 * Collection of partitioning heuristics
 */
package object partition {
    var costHyperedge = 5.0
    var costReplica = 10.0
    var costDemand = 1.0
    var norm = 2

    private[partition] val searchEpsilonFraction = 0.01
}
