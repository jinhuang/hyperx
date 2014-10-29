package org.apache.spark.hyperx.util.collection

import org.apache.spark.util.collection.{OpenHashMap, BitSet}

class HyperXLongBitSet extends Iterable[Long] with Serializable {

    def add(value: Long): Unit ={
        setValue(value, flag = true)
    }

    def remove(value: Long): Unit = {
        setValue(value, flag = false)
    }

    def contains(value: Long): Boolean = {
        getValue(value)
    }

    override def iterator: Iterator[Long] = {
        sets.flatMap{entry =>
            val bitset = entry._2
            val baseIndex = entry._1 << valuebits
            bitset.iterator.map(_.toLong + baseIndex)
        }.iterator
    }

    private val valuebits = 4
    private val valuemask = (1 << valuebits) - 1

    private[hyperx] val sets = new OpenHashMap[Long, BitSet](valuebits)

    private def getSetIndex(index: Long): Long = {
        index >> valuebits
    }

    private def getPos(index: Long): Int = {
        (index & valuemask).toInt
    }

    private def bitset(index: Long): BitSet = {
        val setIndex = getSetIndex(index)
        var set = sets(setIndex)
        set = set match {
            case existing: BitSet =>
                existing
            case null =>
//                val newSet = new BitSet(1024)
                val newSet = new BitSet(1 << valuebits)
                sets.update(setIndex, newSet)
                newSet
        }
        set
    }

    private def setValue(index: Long, flag: Boolean): Unit ={
        if (flag) {
//            growIfNeeded(index)
            bitset(index).set(getPos(index))
        }
        else {
            val set = sets(getSetIndex(index))
            set match {
                case existing: BitSet =>
                    val pos = getPos(index)
                    if (pos < existing.capacity) {
                        existing.unset(getPos(index))
                    }
            }
        }
    }

    private def getValue(index: Long): Boolean = {
        val bitset = sets(getSetIndex(index))
        bitset != null && bitset.get(getPos(index))
    }

//    private def growIfNeeded(index: Long): Unit = {
//        val pos = getPos(index)
//        val set = bitset(index)
//        if (set.capacity <= pos) {
//            val newSet = new BitSet(pos + 1)
//            set.iterator.foreach(newSet.set)
//            sets.update(getSetIndex(index), newSet)
//        }
//    }
}
