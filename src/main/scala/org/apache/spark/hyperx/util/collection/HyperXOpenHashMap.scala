/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.hyperx.util.collection

import scala.reflect._

/**
 * A fast hash map implementation for primitive, non-null keys. This hash map
 * supports insertions and updates, but not deletions. This map is about an
 * order of magnitude faster than java.util.HashMap, while using much less
 * space overhead.
 *
 * Under the hood, it uses our OpenHashSet implementation.
 *
 * There is a bug in the original Spark 1.1.0 implementation:
 * If there are more than 11744053 pairs stored, the update method will
 * miserably fail to set the correct value for the key. Also, the update of the
 * value on an existing key may or may not result to correct set.
 *
 * The bug is caused by wrong pos calculating for a given key: the pos is
 * calculated before rehashing the underlying key set, and the value assigned to
 * that may overwrite the value of an overflow key. To fix the bug, we first
 * add the key, and rehash accordingly; then the pos is calculated based on the
 * new key set and the value is assigned.
 *
 */
private[hyperx]
class HyperXOpenHashMap[@specialized(Long, Int) K: ClassTag,
@specialized(Long, Int, Double) V: ClassTag](val keySet:HyperXOpenHashSet[K],
    var _values: Array[V])
        extends Iterable[(K, V)]
        with Serializable {

    // The following member variables are declared as protected instead of
    // private for the  specialization to work (specialized class extends the
    // unspecialized one and needs access to the "private" variables).
    // They also should have been val's. We use var's because there is a Scala
    // compiler bug that would throw illegal access error at runtime if they are
    // declared as val's.
    protected var grow = (newCapacity: Int) => {
        _oldValues = _values
        _values = new Array[V](newCapacity)
    }
    protected var move = (oldPos: Int, newPos: Int) => {
        _values(newPos) = _oldValues(oldPos)
    }
    private var _oldValues: Array[V] = null

//    require(classTag[K] == classTag[Long] || classTag[K] == classTag[Int])

    /**
     * Allocate an OpenHashMap with a fixed initial capacity
     */
    def this(initialCapacity: Int) =
        this(new HyperXOpenHashSet[K](initialCapacity),
            new Array[V](initialCapacity))

    /**
     * Allocate an OpenHashMap with a fixed initial capacity
     */
    def this(keySet: HyperXOpenHashSet[K]) = this(keySet,
        new Array[V](keySet.capacity))

    override def size = keySet.size

    /** Get the value for a given key */
    def apply(k: K): V = {
        val pos = keySet.getPos(k)
        // _values(pos)
        // fix array index out of bound
        if (pos >= 0 && _values.size > pos) {
            _values(pos)
        }
        else null.asInstanceOf[V]
    }

//    def nth(n: Int): V = {
//        if (n >= keySet.size) {
//            null.asInstanceOf[V]
//        }
//        else {
//            var pos = keySet.nextPos(0)
//            (0 until n).foreach { i => pos = keySet.nextPos(pos + 1)}
//            _values(pos)
//        }
//    }

    def nextPos(pos: Int) = {
        keySet.nextPos(pos)
    }

    /** Get the value for a given key, or returns elseValue if it doesn't
      * exist. */
    def getOrElse(k: K, elseValue: V): V = {
        val pos = keySet.getPos(k)
        if (pos >= 0) _values(pos) else elseValue
    }

    /** Set the value for a key */
    def setMerge(k: K, v: V, mergeF: (V, V) => V) {
//        val pos = keySet.addWithoutResize(k)
        keySet.addWithoutResize(k)
        keySet.rehashIfNeeded(k, grow, move)
        val pos = keySet.getPos(k)
        val ind = pos & HyperXOpenHashSet.POSITION_MASK
        if ((pos & HyperXOpenHashSet.NONEXISTENCE_MASK) != 0) {
            // if first add
            _values(ind) = v
        } else {
            _values(ind) = mergeF(_values(ind), v)
        }
//        keySet.rehashIfNeeded(k, grow, move)
        _oldValues = null
    }


    /**
     * If the key doesn't exist yet in the hash map, set its value to
     * defaultValue; otherwise, set its value to mergeValue(oldValue).
     *
     * @return the newly updated value.
     */
    def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V = {
        if (!keySet.contains(k)) {
            keySet.addWithoutResize(k)
            keySet.rehashIfNeeded(k, grow, move)
            val pos = keySet.getPos(k)
            _values(pos) = defaultValue
            _values(pos)
        } else {
            val pos = keySet.getPos(k)
            _values(pos) = mergeValue(_values(pos))
            _values(pos)
        }
    }

    def ++(other: HyperXOpenHashMap[K, V]): HyperXOpenHashMap[K, V] = {
        val merged = new HyperXOpenHashMap[K, V]()
        val it: Iterator[(K, V)] = this.iterator ++ other.iterator
        while (it.hasNext) {
            val cur = it.next()
            merged.update(cur._1, cur._2)
        }
        merged
    }

    /**
     * Allocate an OpenHashMap with a default initial capacity, providing a true
     * no-argument constructor.
     */
    def this() = this(64)

    /** Set the value for a key */
    def update(k: K, v: V) {
        // fix the wrong pos set
        keySet.addWithoutResize(k)
        keySet.rehashIfNeeded(k, grow, move)
        _oldValues = null
        val pos = keySet.getPos(k)
        _values(pos) = v
    }

    override def iterator = new Iterator[(K, V)] {
        var pos = 0
        var nextPair: (K, V) = computeNextPair()

        /** Get the next value we should return from next(),
          * or null if we're finished iterating */
        def computeNextPair(): (K, V) = {
            pos = keySet.nextPos(pos)
            if (pos >= 0) {
                val ret = (keySet.getValue(pos), _values(pos))
                pos += 1
                ret
            } else {
                null
            }
        }

        def hasNext = nextPair != null

        def next() = {
            val pair = nextPair
            nextPair = computeNextPair()
            pair
        }
    }

    def hasKey(k: K): Boolean = keySet.contains(k)

    def clear(): Unit = {
        keySet.clear()
        _values = null
    }

    def toArrays: (Array[K], Array[V]) = {
        val keyAry = new HyperXPrimitiveVector[K]()
        val valAry = new HyperXPrimitiveVector[V]()
        iterator.foreach{pair=>
            keyAry += pair._1
            valAry += pair._2
        }
        (keyAry.trim().array, valAry.trim().array)
    }

    def mapOn(f: (V) => V): HyperXOpenHashMap[K, V] = {
        this.iterator.foreach{pair=>
            this.update(pair._1, f(pair._2))
        }
        this
    }

    def mapOn(f: (K, V) => V): HyperXOpenHashMap[K, V] = {
        this.iterator.foreach{pair=>
            this.update(pair._1, f(pair._1, pair._2))
        }
        this
    }
}

private[hyperx]
object HyperXOpenHashMap {
    def apply[K: ClassTag, V: ClassTag](iter: Iterator[(K, V)]): HyperXOpenHashMap[K, V] = {
        val map = new HyperXOpenHashMap[K, V]()
        iter.foreach(pair => map.update(pair._1, pair._2))
        map
    }
}
