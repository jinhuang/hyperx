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

package org.apache.spark.hyperx.lib

import java.util

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Matrix => BM,
SparseVector => BSV, Vector => BV, axpy => brzAxpy, svd => brzSvd,
MatrixNotSquareException, MatrixNotSymmetricException, NotConvergedException,
lowerTriangular}
import breeze.numerics.{sqrt => brzSqrt}
import com.github.fommil.netlib.ARPACK
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.hyperx.lib.MllibRDDFunctions._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.storage.StorageLevel
import org.netlib.util.{doubleW, intW}

@Experimental
private[hyperx] object Linalg extends Logging {

    /**
     * Computes the Gramian matrix `A^T A`.
     */
    def computeGramianMatrix(matrix: RowMatrix): Matrix = {
        val n = matrix.numCols.toInt
        checkNumColumns(n)
        // Computes n*(n+1)/2, avoiding overflow in the multiplication.
        // This succeeds when n <= 65535, which is checked above
        val nt: Int = if (n % 2 == 0) ((n / 2) * (n + 1)) else (n * ((n + 1) / 2))

        // Compute the upper triangular part of the gram matrix.
        val GU = matrix.rows.treeAggregate(new BDV[Double](new Array[Double](nt)))(
            seqOp = (U, v) => {
                dspr(1.0, v, U.data)
                U
            }, combOp = (U1, U2) => U1 += U2)

        triuToFull(n, GU.data)
    }

    private def checkNumColumns(cols: Int): Unit = {
        if (cols > 65535) {
            throw new IllegalArgumentException(s"Argument with more than 65535 cols: $cols")
        }
        if (cols > 10000) {
            val mem = cols * cols * 8
            logWarning(s"$cols columns will require at least $mem bytes of memory!")
        }
    }

    def parallelSVD(matrix: RowMatrix,
                      k: Int,
                      computeU: Boolean = false,
                      rCond: Double = 1e-9): SingularValueDecomposition[RowMatrix, Matrix] = {
        // maximum number of Arnoldi update iterations for invoking ARPACK
        val maxIter = math.max(300, k * 3)
        // numerical tolerance for invoking ARPACK
        val tol = 1e-10
        parallelSVD(matrix, k, computeU, rCond, maxIter, tol, "auto")
    }

    def parallelSVD(matrix: RowMatrix,
                                     k: Int,
                                     computeU: Boolean,
                                     rCond: Double,
                                     maxIter: Int,
                                     tol: Double,
                                     mode: String): SingularValueDecomposition[RowMatrix, Matrix] = {
        val n = matrix.numCols.toInt
        require(k > 0 && k <= n, s"Request up to n singular values but got k=$k and n=$n.")

        object SVDMode extends Enumeration {
            val LocalARPACK, LocalLAPACK, DistARPACK = Value
        }

        val computeMode = mode match {
            case "auto" =>
                // TODO: The conditions below are not fully tested.
                if (n < 100 || k > n / 2) {
                    // If n is small or k is large compared with n, we better compute the Gramian matrix first
                    // and then compute its eigenvalues locally, instead of making multiple passes.
                    if (k < n / 3) {
                        SVDMode.LocalARPACK
                    } else {
                        SVDMode.LocalLAPACK
                    }
                } else {
                    // If k is small compared with n, we use ARPACK with distributed multiplication.
                    SVDMode.DistARPACK
                }
            case "local-svd" => SVDMode.LocalLAPACK
            case "local-eigs" => SVDMode.LocalARPACK
            case "dist-eigs" => SVDMode.DistARPACK
            case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
        }

        // Compute the eigen-decomposition of A' * A.
        val (sigmaSquares: BDV[Double], u: BDM[Double]) = computeMode match {
            case SVDMode.LocalARPACK =>
                require(k < n, s"k must be smaller than n in local-eigs mode but got k=$k and n=$n.")
                val G = toBreeze(computeGramianMatrix(matrix)).asInstanceOf[BDM[Double]]
                parallelEig(v => G * v, n, k, tol, maxIter)
            case SVDMode.LocalLAPACK =>
                val G = toBreeze(computeGramianMatrix(matrix)).asInstanceOf[BDM[Double]]
                val brzSvd.SVD(uFull: BDM[Double], sigmaSquaresFull: BDV[Double], _) = brzSvd(G)
                (sigmaSquaresFull, uFull)
            case SVDMode.DistARPACK =>
                if (matrix.rows.getStorageLevel == StorageLevel.NONE) {
                    logWarning("The input data is not directly cached, which may hurt performance if its"
                        + " parent RDDs are also uncached.")
                }
                require(k < n, s"k must be smaller than n in dist-eigs mode but got k=$k and n=$n.")
                parallelEig(multiplyGramianMatrixBy(matrix), n, k, tol, maxIter)
        }

        val sigmas: BDV[Double] = brzSqrt(sigmaSquares)

        // Determine the effective rank.
        val sigma0 = sigmas(0)
        val threshold = rCond * sigma0
        var i = 0
        // sigmas might have a length smaller than k, if some Ritz values do not satisfy the convergence
        // criterion specified by tol after max number of iterations.
        // Thus use i < min(k, sigmas.length) instead of i < k.
        if (sigmas.length < k) {
            logWarning(s"Requested $k singular values but only found ${sigmas.length} converged.")
        }
        while (i < math.min(k, sigmas.length) && sigmas(i) >= threshold) {
            i += 1
        }
        val sk = i

        if (sk < k) {
            logWarning(s"Requested $k singular values but only found $sk nonzeros.")
        }

        // Warn at the end of the run as well, for increased visibility.
        if (computeMode == SVDMode.DistARPACK && matrix.rows.getStorageLevel == StorageLevel.NONE) {
            logWarning("The input data was not directly cached, which may hurt performance if its"
                + " parent RDDs are also uncached.")
        }

        val s = Vectors.dense(util.Arrays.copyOfRange(sigmas.data, 0, sk))
        val V = Matrices.dense(n, sk, util.Arrays.copyOfRange(u.data, 0, n * sk))

        if (computeU) {
            // N = Vk * Sk^{-1}
            val N = new BDM[Double](n, sk, util.Arrays.copyOfRange(u.data, 0, n * sk))
            var i = 0
            var j = 0
            while (j < sk) {
                i = 0
                val sigma = sigmas(j)
                while (i < n) {
                    N(i, j) /= sigma
                    i += 1
                }
                j += 1
            }
            val U = matrix.multiply(fromBreeze(N))
            SingularValueDecomposition(U, s, V)
        } else {
            SingularValueDecomposition(null, s, V)
        }
    }

    private[hyperx] def localEig(matrix: BM[Double]): (BDV[Double], BDM[Double]) = {

//        requireSymmetricMatrix(matrix)

        val A = lowerTriangular(matrix)
        val N = matrix.rows
        val evs = BDV.zeros[Double](N)
        val lwork = scala.math.max(1, 3*N - 1)
        val work = Array.ofDim[Double](lwork)
        val info = new intW(0)

        lapack.dsyev(
            "V",
            "L",
            N, A.data, scala.math.max(1, N),
            evs.data,
            work, lwork,
            info
        )
        assert(info.`val` >= 0)

        if (info.`val` >0)
            throw new NotConvergedException(NotConvergedException.Iterations)


        val sorted = (0 until N).map(i => (evs(i), A.toArray.slice(i * N, (i + 1) * N))).sortBy(0 - _._1).toArray

        val sortedEV = new BDV[Double](sorted.map(_._1).toArray)
        val sortedA = new BDM[Double](N, N, sorted.map(_._2).reduce(_ ++ _))
        (sortedEV, sortedA)
    }

    def requireSymmetricMatrix(mat: BM[Double]): Unit = {
        if (mat.rows != mat.cols)
            throw new MatrixNotSquareException

        for (i <- 0 until mat.rows; j <- 0 until i)
            if (mat(i, j) != mat(j, i))
                throw new MatrixNotSymmetricException
    }

    /**
     * Compute the leading k eigenvalues and eigenvectors on a symmetric square matrix using ARPACK.
     * The caller needs to ensure that the input matrix is real symmetric. This function requires
     * memory for `n*(4*k+4)` doubles.
     *
     * @param mul a function that multiplies the symmetric matrix with a DenseVector.
     * @param n dimension of the square matrix (maximum Int.MaxValue).
     * @param k number of leading eigenvalues required, 0 < k < n.
     * @param tol tolerance of the eigs computation.
     * @param maxIterations the maximum number of Arnoldi update iterations.
     * @return a dense vector of eigenvalues in descending order and a dense matrix of eigenvectors
     *         (columns of the matrix).
     * @note The number of computed eigenvalues might be smaller than k when some Ritz values do not
     *       satisfy the convergence criterion specified by tol (see ARPACK Users Guide, Chapter 4.6
     *       for more details). The maximum number of Arnoldi update iterations is set to 300 in this
     *       function.
     */
    private[hyperx] def parallelEig(
                                        mul: BDV[Double] => BDV[Double],
                                        n: Int,
                                        k: Int,
                                        tol: Double,
                                        maxIterations: Int): (BDV[Double], BDM[Double]) = {
        // TODO: remove this function and use eigs in breeze when switching breeze version
        require(n > k, s"Number of required eigenvalues $k must be smaller than matrix dimension $n")

        val arpack = ARPACK.getInstance()

        // tolerance used in stopping criterion
        val tolW = new doubleW(tol)
        // number of desired eigenvalues, 0 < nev < n
        val nev = new intW(k)
        // nev Lanczos vectors are generated in the first iteration
        // ncv-nev Lanczos vectors are generated in each subsequent iteration
        // ncv must be smaller than n
        val ncv = math.min(4 * k, n)

        // "I" for standard eigenvalue problem, "G" for generalized eigenvalue problem
        val bmat = "I"
        // "LM" : compute the NEV largest (in magnitude) eigenvalues
        val which = "LM"

        var iparam = new Array[Int](11)
        // use exact shift in each iteration
        iparam(0) = 1
        // maximum number of Arnoldi update iterations, or the actual number of iterations on output
        iparam(2) = maxIterations
        // Mode 1: A*x = lambda*x, A symmetric
        iparam(6) = 1

        var ido = new intW(0)
        var info = new intW(0)
        var resid = new Array[Double](n)
        var v = new Array[Double](n * ncv)
        var workd = new Array[Double](n * 3)
        var workl = new Array[Double](ncv * (ncv + 8))
        var ipntr = new Array[Int](11)

        // call ARPACK's reverse communication, first iteration with ido = 0
        arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr, workd,
            workl, workl.length, info)

        val w = BDV(workd)

        // ido = 99 : done flag in reverse communication
        while (ido.`val` != 99) {
            if (ido.`val` != -1 && ido.`val` != 1) {
                throw new IllegalStateException("ARPACK returns ido = " + ido.`val` +
                    " This flag is not compatible with Mode 1: A*x = lambda*x, A symmetric.")
            }
            // multiply working vector with the matrix
            val inputOffset = ipntr(0) - 1
            val outputOffset = ipntr(1) - 1
            val x = w.slice(inputOffset, inputOffset + n)
            val y = w.slice(outputOffset, outputOffset + n)
            y := mul(x)
            // call ARPACK's reverse communication
            arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr,
                workd, workl, workl.length, info)
        }

        if (info.`val` != 0) {
            info.`val` match {
                case 1 => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
                    " Maximum number of iterations taken. (Refer ARPACK user guide for details)")
                case 3 => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
                    " No shifts could be applied. Try to increase NCV. " +
                    "(Refer ARPACK user guide for details)")
                case _ => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
                    " Please refer ARPACK user guide for error message.")
            }
        }

        val d = new Array[Double](nev.`val`)
        val select = new Array[Boolean](ncv)
        // copy the Ritz vectors
        val z = java.util.Arrays.copyOfRange(v, 0, nev.`val` * n)

        // call ARPACK's post-processing for eigenvectors
        arpack.dseupd(true, "A", select, d, z, n, 0.0, bmat, n, which, nev, tol, resid, ncv, v, n,
            iparam, ipntr, workd, workl, workl.length, info)

        // number of computed eigenvalues, might be smaller than k
        val computed = iparam(4)

        val eigenPairs = java.util.Arrays.copyOfRange(d, 0, computed).zipWithIndex.map { r =>
            (r._1, java.util.Arrays.copyOfRange(z, r._2 * n, r._2 * n + n))
        }

        // sort the eigen-pairs in descending order
        val sortedEigenPairs = eigenPairs.sortBy(- _._1)

        // copy eigenvectors in descending order of eigenvalues
        val sortedU = BDM.zeros[Double](n, computed)
        sortedEigenPairs.zipWithIndex.foreach { r =>
            val b = r._2 * n
            var i = 0
            while (i < n) {
                sortedU.data(b + i) = r._1._2(i)
                i += 1
            }
        }

        (BDV[Double](sortedEigenPairs.map(_._1)), sortedU)
    }

    private[hyperx] def multiplyGramianMatrixBy(matrix: RowMatrix)( v: BDV[Double]): BDV[Double] = {
        val n = matrix.numCols.toInt
        val vbr = matrix.rows.context.broadcast(v)
        matrix.rows.treeAggregate(BDV.zeros[Double](n))(
            seqOp = (U, r) => {
                val rBrz = toBreeze(r)
                val a = rBrz.dot(vbr.value)
                rBrz match {
                    // use specialized axpy for better performance
                    case _: BDV[_] => brzAxpy(a, rBrz.asInstanceOf[BDV[Double]], U)
                    case _: BSV[_] => brzAxpy(a, rBrz.asInstanceOf[BSV[Double]], U)
                    case _ => throw new UnsupportedOperationException(
                        s"Do not support vector operation from type ${rBrz.getClass.getName}.")
                }
                U
            }, combOp = (U1, U2) => U1 += U2)
    }

    private[hyperx] def fromBreeze(breeze: BM[Double]): Matrix = {
        breeze match {
            case dm: BDM[Double] =>
                require(dm.majorStride == dm.rows,
                    "Do not support stride size different from the number of rows.")
                new DenseMatrix(dm.rows, dm.cols, dm.data)
            case _ =>
                throw new UnsupportedOperationException(
                    s"Do not support conversion from type ${breeze.getClass.getName}.")
        }
    }

    private def triuToFull(n: Int, U: Array[Double]): Matrix = {
        val G = new BDM[Double](n, n)

        var row = 0
        var col = 0
        var idx = 0
        var value = 0.0
        while (col < n) {
            row = 0
            while (row < col) {
                value = U(idx)
                G(row, col) = value
                G(col, row) = value
                idx += 1
                row += 1
            }
            G(col, col) = U(idx)
            idx += 1
            col +=1
        }

        Matrices.dense(n, n, G.data)
    }

    /**
     * Adds alpha * x * x.t to a matrix in-place. This is the same as BLAS's DSPR.
     *
     * @param U the upper triangular part of the matrix packed in an array (column major)
     */
    def dspr(alpha: Double, v: Vector, U: Array[Double]): Unit = {
        // TODO: Find a better home (breeze?) for this method.
        val n = v.size
        v match {
            case dv: DenseVector =>

                blas.dspr("U", n, alpha, dv.values, 1, U)
            case sv: SparseVector =>
                val indices = sv.indices
                val values = sv.values
                val nnz = indices.length
                var colStartIdx = 0
                var prevCol = 0
                var col = 0
                var j = 0
                var i = 0
                var av = 0.0
                while (j < nnz) {
                    col = indices(j)
                    // Skip empty columns.
                    colStartIdx += (col - prevCol) * (col + prevCol + 1) / 2
                    col = indices(j)
                    av = alpha * values(j)
                    i = 0
                    while (i <= j) {
                        U(colStartIdx + indices(i)) += av * values(i)
                        i += 1
                    }
                    j += 1
                    prevCol = col
                }
        }
    }

    def toBreeze(vector: Vector): BV[Double] = {
        vector match {
            case dense: DenseVector =>
                new BDV[Double](dense.toArray)
            case sparse: SparseVector =>
                new BSV[Double](sparse.indices, sparse.values, sparse.size)
        }
    }

    def toBreeze(matrix: Matrix): BM[Double] =
        new BDM[Double](matrix.numRows, matrix.numCols, matrix.toArray)
}