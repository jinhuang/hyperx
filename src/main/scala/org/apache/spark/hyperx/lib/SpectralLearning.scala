package org.apache.spark.hyperx.lib

import breeze.linalg.svd.SVD
import breeze.linalg.{sum, svd, DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, Vector => BV}
import org.apache.spark.SparkContext._
import org.apache.spark.hyperx.Hypergraph
import org.apache.spark.hyperx.util.collection.HyperXPrimitiveVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Given a hypergraph, first compute its normalized Laplacian, then computes
 * the largest k eigen vectors and eigen values
 * (via Lanczos-Selective Orthogonalization) for clustering and embedding
 *
 * Only supports int vertices
 */
object SpectralLearning extends Logging{

    type VertexSparseMatrix = RDD[(VID, (Array[VID], Array[Double]))]
    type VertexDenseMatrix = RDD[(VID, Array[Double])]
    type VID = Int

    var sc: SparkContext = _

    def run[VD: ClassTag, ED: ClassTag](hypergraph: Hypergraph[VD, ED],
        eigenK: Int, numIter: Int, tol: Double): (Array[Double], Array[Double]) = {
        sc = hypergraph.vertices.context
        val laplacian = hypergraph.laplacian.map(each => (each._1.toInt, (each._2._1.map(_.toInt), each._2._2))).cache()
        lanczos(laplacian, eigenK, numIter, tol)
    }

    private def lanczos(matrix: VertexSparseMatrix, eigenK: Int, numIter: Int, tol: Double): (Array[Double], Array[Double]) = {
        val n = matrix.count().toInt

        val alpha, beta = new HyperXPrimitiveVector[Double]()
        var alphaLast, alphaCur, betaLast, betaCur = 0.0
        var vLast, vCur = BDV.zeros[Double](n)
        val b = new BDV[Double]((0 until n).map(i => Random.nextDouble()).toArray)
        //        val b = new BDV[Double](Array.fill(n)(2.0))
        vCur = (b / norm(b)).asInstanceOf[BDV[Double]]
        var vMatrix = new BDM[Double](n, 1, vCur.toArray)
        var i = 0

        while((i == 0 || Math.abs(betaCur - 0.0) > tol) && i < numIter) {
            val start = System.currentTimeMillis()
            val dimension = i + 1
            var v = matVecMult(matrix, vCur)
            alphaCur = sum(vCur :* v)
            v = v - vCur * alphaCur
            v = v - vLast * betaLast
            betaCur = norm(v)
            alpha += alphaCur
            alphaLast = alphaCur

            val alphaVector = new BDV[Double](alpha.array.slice(0, i + 1))
            val betaVector = new BDV[Double]((beta.array.slice(0, i).iterator ++ Iterator(betaCur)).toArray)

            val t = tridiongonal(alphaVector, betaVector)
            val (_, eigQ) = Linalg.localEig(t)

            var reflag = false
            (0 to i).foreach{j =>
                if (Math.abs(betaCur * eigQ.toArray(j * dimension + i)) <= Math.sqrt(tol) * l2Norm(t)) {
                    reflag = true
                    val rMatrix = vMatrix * new BDM[Double](dimension, 1, eigQ.toArray.slice(j *
                        dimension, (j + 1) * dimension))
                    val r = new BDV[Double](rMatrix.asInstanceOf[BDM[Double]].toArray)
                    v = v - r * sum(r :* v)
                }
            }

            if (reflag) {
                betaCur = norm(v)
            }
            beta += betaCur
            betaLast = betaCur
            vLast = vCur
            vCur = v / betaCur
            i += 1
            vMatrix = new BDM[Double](n, i + 1, vMatrix.toArray ++ vCur.toArray)
            val duration = System.currentTimeMillis() - start
            println(s"HYPERX: iterator $i with beta $betaCur in $duration ms")
        }

        val alphaVector = new BDV[Double](alpha.array.slice(0, i))
        val betaVector = new BDV[Double](beta.array.slice(0, i))
        val t = tridiongonal(alphaVector, betaVector)
        val (eigV, eigQ) = Linalg.localEig(t)
        vMatrix = new BDM[Double](n, i, vMatrix.toArray.slice(0, i * n))
        val retVal = eigV.toArray.slice(0, eigenK)
        val retVec = (vMatrix * new BDM[Double](i, eigenK, eigQ.toArray.slice(0, eigenK * i))).asInstanceOf[BDM[Double]].toArray
        (retVal, retVec)
    }

    private def tridiongonal(alpha: BV[Double], beta: BV[Double]): BM[Double] = {
        val n = alpha.size
        val values = (0 until n).flatMap{col =>
            (0 until n).map{row =>
                if (col == row) {
                    alpha(col)
                } else if (col == row - 1) {
                    beta(col)
                } else if (col == row + 1) {
                    beta(row)
                } else 0.0
            }
        }.toArray
        new BDM[Double](n, n, values)
    }

    private def norm(vector: BV[Double]): Double = {
        Math.sqrt(vector.valuesIterator.map(d => Math.pow(d, 2)).sum)
    }

    private def l2Norm(matrix: BM[Double]): Double = {
        val SVD(_, s, _) = svd(matrix.asInstanceOf[BDM[Double]])
        s.toArray.max
    }

    private def matVecMult(matrix: VertexSparseMatrix, vector: BDV[Double]): BDV[Double] = {
        val map = matrix.filter(row => row._2 != null && row._2._1 != null).flatMap{row =>
            val rowId = row._1
            val rowSize = row._2._1.size
            val colArray = new HyperXPrimitiveVector[(VID, Double)]()
            val rowSum = (0 until rowSize).map{i =>
                val colId = row._2._1(i)
                val oldVal = row._2._2(i)
                colArray += (colId, oldVal * vector(rowId))
                oldVal * vector(colId)
            }.sum
            Iterator((rowId, rowSum)) ++ colArray.trim().array.iterator
        }.reduceByKey(_ + _).collectAsMap()
        new BDV[Double]((0 until vector.length).map(i => map.getOrElse(i, 0.0)).toArray)
    }
}
