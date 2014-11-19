package org.apache.spark.hyperx.lib

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Machine learning specific RDD functions.
 */
@DeveloperApi
class MllibRDDFunctions[T: ClassTag](self: RDD[T]) extends Serializable {

    /**
     * Returns a RDD from grouping items of its parent RDD in fixed size blocks by passing a sliding
     * window over them. The ordering is first based on the partition index and then the ordering of
     * items within each partition. This is similar to sliding in Scala collections, except that it
     * becomes an empty RDD if the window size is greater than the total number of items. It needs to
     * trigger a Spark job if the parent RDD has more than one partitions and the window size is
     * greater than 1.
     */
    def sliding(windowSize: Int): RDD[Array[T]] = {
        require(windowSize > 0, s"Sliding window size must be positive, but got $windowSize.")
        if (windowSize == 1) {
            self.map(Array(_))
        } else {
            new SlidingRDD[T](self, windowSize)
        }
    }

    /**
     * Reduces the elements of this RDD in a multi-level tree pattern.
     *
     * @param depth suggested depth of the tree (default: 2)
     * @see [[org.apache.spark.rdd.RDD#reduce]]
     */
    def treeReduce(f: (T, T) => T, depth: Int = 2): T = {
        require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
        val cleanF = self.context.clean(f)
        val reducePartition: Iterator[T] => Option[T] = iter => {
            if (iter.hasNext) {
                Some(iter.reduceLeft(cleanF))
            } else {
                None
            }
        }
        val partiallyReduced = self.mapPartitions(it => Iterator(reducePartition(it)))
        val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
            if (c.isDefined && x.isDefined) {
                Some(cleanF(c.get, x.get))
            } else if (c.isDefined) {
                c
            } else if (x.isDefined) {
                x
            } else {
                None
            }
        }
        MllibRDDFunctions.fromRDD(partiallyReduced).treeAggregate(Option.empty[T])(op, op, depth)
            .getOrElse(throw new UnsupportedOperationException("empty collection"))
    }

    /**
     * Aggregates the elements of this RDD in a multi-level tree pattern.
     *
     * @param depth suggested depth of the tree (default: 2)
     * @see [[org.apache.spark.rdd.RDD#aggregate]]
     */
    def treeAggregate[U: ClassTag](zeroValue: U)(
        seqOp: (U, T) => U,
        combOp: (U, U) => U,
        depth: Int = 2): U = {
        require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
        if (self.partitions.size == 0) {
            return Utils.clone(zeroValue, self.context.env.closureSerializer.newInstance())
        }
        val cleanSeqOp = self.context.clean(seqOp)
        val cleanCombOp = self.context.clean(combOp)
        val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
        var partiallyAggregated = self.mapPartitions(it => Iterator(aggregatePartition(it)))
        var numPartitions = partiallyAggregated.partitions.size
        val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
        // If creating an extra level doesn't help reduce the wall-clock time, we stop tree aggregation.
        while (numPartitions > scale + numPartitions / scale) {
            numPartitions /= scale
            val curNumPartitions = numPartitions
            partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex { (i, iter) =>
                iter.map((i % curNumPartitions, _))
            }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values
        }
        partiallyAggregated.reduce(cleanCombOp)
    }
}

@DeveloperApi
object MllibRDDFunctions {

    /** Implicit conversion from an RDD to RDDFunctions. */
    implicit def fromRDD[T: ClassTag](rdd: RDD[T]) = new MllibRDDFunctions[T](rdd)
}