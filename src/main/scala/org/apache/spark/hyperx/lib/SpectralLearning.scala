package org.apache.spark.hyperx.lib

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{sqrt => brzSqrt}
import org.apache.spark.SparkContext
import org.apache.spark.hyperx.util.collection.HyperXPrimitiveVector
import org.apache.spark.hyperx.{Hypergraph, VertexId, VertexRDD}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg._

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Given a hypergraph, first compute its normalized Laplacian, then computes
 * the largest k eigen vectors and eigen values
 * (via Lanczos-Selective Orthogonalization) for clustering and embedding
 */
object SpectralLearning {

    type VertexMatrix = VertexRDD[Map[VertexId, Double]]
    type BasisMatrix = VertexRDD[Array[Double]]

    def run[VD: ClassTag, ED: ClassTag](hypergraph: Hypergraph[VD, ED],
        eigenK: Int, numIter: Int, tol: Double): (Array[Double], VertexRDD[Array[Double]]) = {
        val laplacian = hypergraph.laplacian
        lanczosSO(laplacian, eigenK, numIter, tol)
    }

    def lanczosSO(matrix: VertexMatrix, eigenK: Int,
        maxIter: Int, tol: Double): (Array[Double], VertexRDD[Array[Double]]) = {

        val n = matrix.count().toInt
        val sc = matrix.context
        val tolSqrt = Math.sqrt(tol)

        // randomize a n-vector
        val alpha, beta = new HyperXPrimitiveVector[Double]()
        var allV: BasisMatrix = matrix.mapValues(each => Array.empty)
        var prevAlpha, currAlpha, prevBeta, currBeta = 0.0
        var prevV, currV = Vectors.zeros(n)
        val b = randomVector(n)
        val normB = vectorL2Norm(b)
        currV = Vectors.dense(b.toArray.map(d => d / normB))
        var i = 0

        // loop
        while(i == 0 || !~=(currBeta, 0.0)) {
            var v = *(matrix, currV)    // parallel
            currAlpha = vectorInner(currV, v)
            val prevBetaV = prevV.toArray.map(d => d * prevBeta)
            val currAlphaV = currV.toArray.map(d => d * currAlpha)
            v = Vectors.dense((0 until v.size).map(i => v(i) - prevBetaV(i) - currAlphaV(i)).toArray)
            currBeta = vectorL2Norm(v)  // in-core
            alpha += currAlpha
            beta += currBeta
            val t = tridiagonal(Vectors.dense(alpha.trim().array), Vectors.dense(beta.trim().array), sc)
            val (_, eigQ) = eigenDecompose(t, eigenK, tol)
            var reflag = false
            (0 to i).foreach{j =>
                if (currBeta * entry(eigQ, i, j, i) <= tolSqrt * matrixL2Norm(t)) {
                    val r = *(allV, col(eigQ, j, i)) // parallel
                    val rv = vectorInner(r, v)
                    v = Vectors.dense((0 until v.size).map(i => v(i) - rv * r(i)).toArray)
                    reflag = true
                }
            }
            if (reflag) {
                currBeta = vectorL2Norm(v)
            }
            currV = Vectors.dense(v.toArray.map(d => d / currBeta))
            allV = addVectorToBasisMatrix(currV, allV)
            i += 1
            prevV = currV
            prevAlpha = currAlpha
            prevBeta = currBeta
        }

        val m = i

        // compute the eigen values and vectors
        val t = tridiagonal(Vectors.dense(alpha.trim().array), Vectors.dense(beta.trim().array), sc)
        val (eigD, eigQ) = eigenDecompose(t, eigenK, tol)
        val effectiveK = if (eigenK > eigD.size) eigD.size else eigenK
        val eigVal = (0 until effectiveK).map(i => eigD(i)).toArray
        val eigVec = *#(allV, (0 until effectiveK).map(i => col(eigQ, i, m)).toArray)
        (eigVal, eigVec)
    }

    /* return the L2 Norm for a given vector*/
    private def vectorL2Norm(vector: Vector): Double = {
        Math.sqrt((0 until vector.size).map(i => Math.pow(vector(i), 2)).sum)
    }

    /* return the inner product of two vectors */
    private def vectorInner(a: Vector, b: Vector): Double = {
        val length = a.size
        (0 until length).map(i => a(i) * b(i)).sum
    }

    /**
     * Return a tri-diagonal matrix constructed from two vectors
     * @param alpha the diagonal vector, i * 1
     * @param beta the upper and lower diagonals, beta(0) is not used, i * 1
     * @return a tri-diagonal matrix, i * i
     */
    private def tridiagonal(alpha: Vector, beta: Vector, sc: SparkContext): RowMatrix = {
        val m = alpha.size
        new RowMatrix(sc.parallelize(0 until m).map{i =>
            if (i == 0) { // first row
                Vectors.sparse(m, Array(0, 1), Array(alpha(0), beta(1)))
            } else if (i == m - 1) { // last row
                Vectors.sparse(m, Array(m - 2, m - 1), Array(beta(m - 1), alpha(m - 1)))
            } else { // in-between rows
                Vectors.sparse(m, Array(i - 1, i, i + 1), Array(beta(i), alpha(i), beta(i + 1)))
            }
        })
    }

    /**
     * Get the (i, j) entry from a matrix
     * @param matrix the matrix
     * @param i the coordinate
     * @param j the coordinate
     * @param n the number of rows in the matrix
     * @return
     */
    private def entry(matrix: Matrix, i: Int, j: Int, n: Int): Double = {
        matrix.toArray(j * n + 1)
    }

    /**
     * Get the jth column from a matrix
     * @param matrix the matrix
     * @param j the coordinate
     * @param n the number of rows in the matrix
     * @return
     */
    private def col(matrix: Matrix, j: Int, n: Int): Vector = {
        Vectors.dense(matrix.toArray.slice(j * n, j * (n + 1)))
    }

    /**
     * Add a vector into a RDD(VectorId, Array[Double]), generate a new RDD
     * @param vector
     * @param matrix
     * @return
     */
    private def addVectorToBasisMatrix(vector: Vector, matrix: BasisMatrix): BasisMatrix = {
        matrix.mapValues((vid, array)=> (array.iterator ++ Iterator(vector(vid.toInt))).toArray)
    }


    private def matrixL2Norm(matrix: RowMatrix): Double = {
        matrix.computeSVD(1, computeU = false, 1e-9).s.toArray.max
    }

    /**
     * Compute the eigen decomposition of a small matrix in-core, using APRACK
     * @param matrix
     * @return
     */
    private def eigenDecompose(matrix: RowMatrix, eigenK: Int, tol : Double): (Vector, Matrix) = {
        // first convert the matrix to breeze matrix
        val rows = matrix.rows.collect().map(vector => vector.toArray)
        val numRows = rows.size
        val numCols = rows(0).size
        val values = (0 until numCols).flatMap{j =>
            (0 until numRows).map{i =>
                rows(i)(j)
            }
        }.toArray
        val bdm = new BDM[Double](numRows, numCols, values)
        val (sigmaSquares: BDV[Double], u: BDM[Double]) =
            EigenValueDecomposition.symmetricEigs(v => bdm * v, numCols, eigenK, tol, IN_CORE_EIGEN_ITER)

        val sigmas = brzSqrt(sigmaSquares)


        val eigenD = Vectors.dense(sigmas.toArray)
        val eigenQ = Matrices.dense(numRows, numCols, bdm.valuesIterator.toArray)

        (eigenD, eigenQ)
    }

    private def randomVector(n: Int): Vector = {
        Vectors.dense((0 until n).map(i => Random.nextDouble()).toArray)
    }

    private def ~= (a: Double, b: Double): Boolean = {
        Math.abs(a - b) < 1e-6
    }

    private def !~= (a: Double, b: Double): Boolean = {
        !(~=(a, b))
    }

    /**
     * Multiple a vertex matrix by a vector
     * @param matrix a n * n matrix
     * @param vector a n * 1 vector, assuming the index corresponds to the vertexId
     * @return
     */
    private def *(matrix: VertexMatrix, vector: Vector): Vector = {
        Vectors.dense(*#(matrix, vector).collect().sortBy(_._1).map(_._2))
    }

    /**
     * Parallel multiple a vertex matrix by a vector
     * @param matrix a n * n sparse matrix
     * @param vector a n * 1 vector
     * @return
     *
     * @todo need to see how to take care of symmetric matrix stored in a unconventional manner
     */
    private def *# (matrix: VertexMatrix, vector: Vector): VertexRDD[Double] = {
        val map = vector.toArray.zipWithIndex.toMap
        matrix.mapValues(old => old.map(v => v._2 * map(v._1)).sum)
    }

    /**
     * Multiple a basis matrix by a vector
     * @param matrix a n * i dense matrix
     * @param vector an i * 1 vector
     * @return
     */
    private def *(matrix: BasisMatrix, vector: Vector): Vector = {
        val i = vector.size
        Vectors.dense(matrix.mapValues(old => (0 until i).map(j => old(j) * vector(j)).sum).collect().sortBy(_._1).map(_._2))
    }

    /**
     * Parallel multiple a basis matrix by an array of vectors
     * @param matrix RDD[VertexId, Array[Double] ], a n * i dense matrix
     * @param vectors an i * m matrix, assuming i and m are both small
     * @return
     */
    private def *# (matrix: BasisMatrix, vectors: Array[Vector]): VertexRDD[Array[Double]] = {
        val numVectors = vectors.size
        if (numVectors == 0){
            matrix
        } else {
            val i = vectors(0).size
            matrix.mapValues(old => (0 until i).map(j => (0 until numVectors).map(v => old(j) * vectors(v)(j)).sum).toArray)
        }
    }

    private val IN_CORE_EIGEN_ITER = 10
}
