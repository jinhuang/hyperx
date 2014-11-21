package org.apache.spark.hyperx.exp

import breeze.linalg.svd.SVD
import breeze.linalg.{sum, svd, DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, Vector => BV}
import org.apache.spark.hyperx.lib.Linalg
import org.apache.spark.hyperx.util.collection.HyperXPrimitiveVector

import scala.util.Random

object Profiler {

    private def lanczos(matrix: BM[Double]): (BV[Double], BM[Double]) = {
        val n = matrix.cols
        val tol = 1e-16

        val alpha, beta = new HyperXPrimitiveVector[Double]()
        var alphaLast, alphaCur, betaLast, betaCur = 0.0
        var vLast, vCur = BV.zeros[Double](n)
        val b = new BDV[Double]((0 until n).map(i => Random.nextDouble()).toArray)
//        val b = new BDV[Double](Array.fill(n)(2.0))
        vCur = b / norm(b)
        var vMatrix = new BDM[Double](n, 1, vCur.toArray)
        var i = 0

        while(i == 0 || Math.abs(betaCur - 0.0) > tol) {
            val dimension = i + 1
            var v = matrix * vCur
            alphaCur = sum(vCur :* v)
            v = v - vLast * betaLast - vCur * alphaCur
            betaCur = norm(v)
            println("before so %f".format(betaCur))
            alpha += alphaCur
            alphaLast = alphaCur

            val alphaVector = new BDV[Double](alpha.array.slice(0, i + 1))
            val betaVector = new BDV[Double]((beta.array.slice(0, i).iterator ++ Iterator(betaCur)).toArray)

            val t = tridiongonal(alphaVector, betaVector)
            val (_, eigQ) = Linalg.localEig(t)

            println(eigQ)

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
            println("after so %s %f".format(reflag, betaCur))
            beta += betaCur
            betaLast = betaCur
            vLast = vCur
            vCur = v / betaCur
            i += 1
            vMatrix = new BDM[Double](n, i + 1, vMatrix.toArray ++ vCur.toArray)
            println(s"iterator $i with beta $betaCur")
        }

        val alphaVector = new BDV[Double](alpha.array.slice(0, i))
        val betaVector = new BDV[Double](beta.array.slice(0, i))
        val t = tridiongonal(alphaVector, betaVector)
        val (eigV, eigQ) = Linalg.localEig(t)
        vMatrix = new BDM[Double](n, i, vMatrix.toArray.slice(0, i * n))
        (eigV, vMatrix * eigQ)
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

    def main(args: Array[String]): Unit = {
        val matrix = new BDM[Double](3, 3, Array(1.0, 2.0, 3.0, 2.0, 0.0, 1.0, 3.0, 1.0, 3.0))
        val (localev, localec) = Linalg.localEig(matrix)
        val (eigval, eigvec) = lanczos(matrix)
        println(eigval)
        println(localev)
        println(eigvec)
        println(localec)
//        println(sum(new BDV[Double](Array.fill(3)(1)) :* new BDV[Double](Array.fill(3)(5))))
    }

}
