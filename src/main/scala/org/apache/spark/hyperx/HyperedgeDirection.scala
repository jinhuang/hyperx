package org.apache.spark.hyperx

/**
 * The direction of a directed hyperedge relative to a vertex set
 *
 * Forked from GraphX, modified by Jin Huang
 */
class HyperedgeDirection private(private val name: String) extends
Serializable {

    /**
     * Reverse the direction of a hyperedge.
     */
    def reverse: HyperedgeDirection = this match {
        case HyperedgeDirection.In => HyperedgeDirection.Out
        case HyperedgeDirection.Out => HyperedgeDirection.In
        case HyperedgeDirection.Either => HyperedgeDirection.Either
        case HyperedgeDirection.Both => HyperedgeDirection.Both
    }

    override def toString: String = "HyperedgeDirection." + name

    override def equals(o: Any) = o match {
        case other: HyperedgeDirection => other.name == name
        case _ => false
    }

    override def hashCode = name.hashCode
}

/**
 * A set of [[HyperedgeDirection]]s.
 */
object HyperedgeDirection {
    final val In = new HyperedgeDirection("In")

    final val Out = new HyperedgeDirection("Out")

    final val Either = new HyperedgeDirection("Either")

    final val Both = new HyperedgeDirection("Both")
}
