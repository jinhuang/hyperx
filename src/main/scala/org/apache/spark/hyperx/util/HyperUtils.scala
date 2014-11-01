package org.apache.spark.hyperx.util

import org.apache.spark.hyperx.util.collection.{HyperXLongBitSet, HyperXOpenHashMap}
import org.apache.spark.hyperx.{HyperAttr, PartitionId, VertexId, VertexSet}
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import scala.collection.mutable
import scala.reflect.ClassTag

/** Forked from GraphX 2.10, modified by Jin Huang */
object HyperUtils {

    /**
     * Determine whether two vertex sets equal to each other
     */
    def is(a: VertexSet, b: VertexSet): Boolean = {
        if (a.size != b.size) false
        a.iterator.map(b.contains).reduce(_ && _)
    }

//    def is(a: HyperXOpenHashSet[VertexId], b: VertexSet):
//    Boolean = {
//        if (a.size != b.size) false
//        else a.iterator.map(v => b.contains(v)).reduce(_ && _)
//    }

    def toString(t: (VertexSet, VertexSet)): String = {
        toString(t._1) + ";" + toString(t._2)
    }

    def toString(s: VertexSet): String = {
        s.iterator.toArray.sortBy(v => v).map(v => v.toString) reduce (_ + " " + _)
    }

    def iteratorFromString(s: String): Iterator[VertexId] = {
//        s.split(" ").map(_.toInt).iterator
//        s.split(" ").map(_.toLong).iterator
        setFromString(s).iterator
    }

    def setFromString(s: String): VertexSet = {
        setFromString(s, " ")
    }

    def setFromString(s: String, separator: String): VertexSet = {
//        val inputArray = s.trim.split(separator).map(_.toInt).toVector
        val inputArray = s.trim.split(separator).map(_.toLong).toVector
        val set = new VertexSet()
        inputArray.foreach(set.add)
        set
    }

    def hyperedgeFromHString(s: String): (VertexSet, VertexSet) = {
        val array = s.split(";")
        (setFromString(array(0)), setFromString(array(1)))
    }

    def bitFromString(s: String): (HyperXLongBitSet, HyperXLongBitSet) = {
        val array = s.split(";")
        val srcArray = array(0).trim.split(" ").map(_.toLong).toVector
        val srcSet = new HyperXLongBitSet()
        srcArray.foreach(srcSet.add)
        val dstArray = array(1).trim.split(" ").map(_.toLong).toVector
        val dstSet = new HyperXLongBitSet()
        dstArray.foreach(dstSet.add)
        (srcSet, dstSet)
    }

    def countDegreeFromHString(s: String): Int = {
        val h = hyperedgeFromHString(s)
        h._1.size + h._2.size
    }

    def countDetailDegreeFromHString(s: String): (Int, Int) = {
        val h = hyperedgeFromHString(s)
        (h._1.size, h._2.size)
    }

    def iteratorFromHString(s: String): Iterator[VertexId] = {
//        s.split(";").flatMap(vs => vs.trim.split(" ").map(_.toInt)).iterator
//        s.split(";").flatMap(vs => vs.trim.split(" ").map(_.toLong)).iterator
        val h = hyperedgeFromHString(s)
        h._1.iterator ++ h._2.iterator
    }

    def iteratorFromPartitionedString(s: String): Iterator[VertexId] = {
        val array = s.split(";")
        (array(1).trim.split(" ").map(_.toLong) ++
        array(2).trim.split(" ").map(_.toLong)).iterator
    }

    def pairFromPartitionedString(s: String): (PartitionId, String) = {
        val array = s.split(";")
        Tuple2(array(0).trim.toInt, array(1) + ";" + array(2))
    }

    /**
     * Union the two vertex sets
     */
    def union(a: VertexSet, b: VertexSet): VertexSet = {
        val set = new VertexSet
        (a.iterator ++ b.iterator).foreach(set.add)
        set
    }


    /**
     * Union the two sets stored as two arrays, assuming elements are unique
     * in two sets
     */
    def union[V: ClassTag](a: Array[V], b: Array[V]): Array[V] = {
        val set = new OpenHashSet[V]()
        (a ++ b).foreach(v => set.add(v))
        set.iterator.toArray
    }

    /**
     * Count the number of intersecting elements in the two sets,
     * where one of the set is stored in an array
     */
    def countIntersection(a: VertexSet, b: Array[VertexId]): Int = {
        b.map(e => if(a.contains(e)) 1 else 0).sum
    }

    /**
     * Count the number of intersecting elements in the two sets,
     * where one of the set is stored in an [[OpenHashSet]]
     */
    def countIntersection(a: VertexSet, b: OpenHashSet[VertexId]): Int = {
        a.iterator.map(v => if (b.contains(v)) 1 else 0).sum
    }

    def countIntersect(a: OpenHashSet[VertexId], b: OpenHashSet[VertexId]): Int = {
        a.iterator.count(b.contains)
    }

    def countIntersect(a: Set[VertexId], b: mutable.HashSet[VertexId]): Int = {
        a.iterator.count(b.contains)
    }

    def countIntersect(a: Set[VertexId], b: OpenHashSet[VertexId]): Int = {
        a.iterator.count(b.contains)
    }

    /**
     * Count the elements in a given set
     */
    def count(id: VertexId, set: VertexSet): (VertexId, Int) = (id, set.size)

    /**
     * Compare the size of two vertex sets, return negative if the first is
     * smaller than the second, vice versa
     */
    def compare(a: VertexSet, b: VertexSet): Int = a.size - b.size

    /**
     * Create a new identifier-attribute map for a given vertex identifier set
     */
    def init[VD: ClassTag](vids: VertexSet): HyperAttr[VD] = {
        val attr = new HyperXOpenHashMap[VertexId, VD]()
        val it = vids.iterator
        while (it.hasNext) {
            attr.update(it.next(), null.asInstanceOf[VD])
        }
        attr
    }

    def toBitSet(set: OpenHashSet[Int]) = {
        val bitset = new BitSet(set.iterator.max + 1)
        set.iterator.foreach(bitset.set)
        bitset
    }


    /**
     * Return a string representation for a set of vertex
     * identifier-attribute pairs
     * @param attr the identifier-attribute map
     * @param sep the separator string between each entry in the map
     * @tparam VD the type of the vertex attribute
     * @return the string representation
     */
    def mkString[VD](attr: HyperAttr[VD], sep: String): String = {
        val buf = new StringBuilder
        val it = attr.iterator
        while (it.hasNext) {
            it.next().toString().addString(buf)
            sep.addString(buf)
        }
        buf.toString()
    }

    /**
     * Return a [[VertexSet]] for the elements in the given array
     */
    def arrayToSet(array: Array[VertexId]): VertexSet = {
        val set = new VertexSet()
        array.foreach(set.add)
        set
    }

    /**
     * Return a [[mutable.HashSet]] for the elements in the given array
     */
    def arrayToHashSet[V](array: Array[V]): mutable.HashSet[V] = {
        val set = new mutable.HashSet[V]()
        array.foreach(e => set.add(e))
        set
    }

    /**
     * Return a [[mutable.HashMap]] for the elements in the given array,
     * using the value to record the frequency of the
     * elements in the array
     */
    def arrayToHashMap[K](array: Array[K]): mutable.HashMap[K, Int] = {
        array.map(e => mutable.HashMap[K, Int]((e, 1))).reduce(concatenate[K])
    }

    /**
     * Concatenate two [[mutable.HashMap]] by adding the unprecedented
     * elements and summing the frequencies for the
     * existing elements
     */
    def concatenate[K](a: mutable.HashMap[K, Int], b: mutable.HashMap[K, Int])
    : mutable.HashMap[K, Int] = {
        a.foreach(e => if (b.contains(e._1)) b.update(e._1, b(e._1) + e._2)
                        else b.update(e._1, e._2))
        b
    }

    /**
     * Return an array for the VertexIds stored in the given VertexSet
     */
    def setToArray(s: VertexSet): Array[VertexId] = {
        s.iterator.toArray
    }

    def divide(dividend: Double, map: HyperAttr[Int]): HyperAttr[Double] = {
        val attr = new HyperXOpenHashMap[VertexId, Double]()
        map.foreach(each => attr.update(each._1, if (each._2 > 0) dividend / each._2 else 0))
        attr
    }


    def printMemory(): String = {
        runtime.freeMemory() / 1000.0 / 1000.0 + "; " +
        runtime.totalMemory() / 1000.0/ 1000.0 + "; " +
        runtime.maxMemory() / 1000.0 / 1000.0
    }

    private[hyperx] def avg(array: Array[Int]): Double =
        array.sum * 1.0 / array.length

    private[hyperx] def dvt(array: Array[Int]): Double = {
        val average = avg(array)
        val sum = array.map(e => Math.pow(Math.abs(e - average), 2)).sum
        Math.pow(sum, 1.0 / 2)
    }

    private[hyperx] def effectiveCount(srcCount: Int, dstCount: Int): Int = {
//        (17 + 0.125 * dstCount + 65 + 0.875 * srcCount).toInt]
        (srcCount * 2.5 + dstCount * 1.0).toInt
    }

    private val runtime = Runtime.getRuntime
}
