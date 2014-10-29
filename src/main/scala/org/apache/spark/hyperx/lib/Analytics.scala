package org.apache.spark.hyperx.lib

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.hyperx.partition._
import org.apache.spark.hyperx.{Hypergraph, HypergraphLoader}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkConf, SparkContext, storage}

import scala.collection.mutable

/**
 * The driver program for all the hypergraph algorithms
 */
object Analytics extends Logging {
    def main(args: Array[String]) = {
        if (args.length < 2) {
            System.err.println(
                "Usage: Analytics <taskType> <file> " +
                        "--numPart=<num_partitions> " +
                        "--inputMode=<list|object|plist> " +
                        "--additionalInput=<vertex object file path> " +
                        "--separator=<character> " +
                        "--vertexLevel=<MEMORY_ONLY|MEMORY_ONLY_SER> " +
                        "--hyperedgeLevel=<MEMORY_ONLY|MEMORY_ONLY_SER> " +
                        "--weighted=<true|false> " +
                        "--partStrategy=<Random|OnePass|Local|Alternate|OnePassP|LocalP|AlternateP>" +
                        "--outputPath=<output path> " +
                        "--objectiveH=<double> " +
                        "--objectiveV=<double> " +
                        "--objectiveNorm=<integer> " +
                        "[other options]")
            System.exit(1)
        }

        val taskType = args(0)
        val fname = args(1)
        val optionsList = args.drop(2).map { arg =>
            arg.dropWhile(_ == '-').split('=') match {
                case Array(opt, v) => opt -> v
                case _ => throw new IllegalArgumentException("Invalid " +
                        "argument: " + arg)
            }
        }
        val options = mutable.Map(optionsList: _*)

        def pickPartitioner(v: String): HeuristicPartition = {
            v match {
                case "Plain" => new PlainPartition
                case "Simple" => new SimplePartition
                case "Random" => new RandomPartition
                case "OnePass" => new OnePassSerialPartition
                case "Local" => new LocalSerialPartition
                case "Alternate" => new AlternateSerialPartition
                case "OnePassP" => new OnePassParallelPartition
                case "LocalP" => new LocalParallelPartition
                case "AlternateP" => new AlternateParallelPartition
                case _ => throw new IllegalArgumentException("Invalid " +
                        "PartitionStrategy: " + v)
            }
        }

        val conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer" +
                ".KryoSerializer")
                .set("spark.kryo.registrator", "org.apache.spark.hyperx" +
                ".HypergraphKryoRegistrator")
                .set("spark.locality.wait", "100000")

        val numPart = options.remove("numPart").map(_.toInt).getOrElse {
            println("Set the number of partitions using --numPart.")
            sys.exit(1)
        }

        val inputMode = options.remove("inputMode").getOrElse(listMode)
        val vertexInput = options.remove("additionalInput").getOrElse{
            if (inputMode.equals(objectMode)) {
                println("Set the vertex input using " +
                        "--additionalInput when inputMode is object file")
                sys.exit(1)
            }
            else ""
        }

        val outputPath = options.remove("outputPath").getOrElse(test_path)

        val partitionStrategy = options.remove("partStrategy")
                .map(pickPartitioner).getOrElse(new RandomPartition)
        val hyperedgeStorageLevel = options.remove("hyperedgeLevel")
                .map(StorageLevel.fromString).getOrElse(StorageLevel
                .MEMORY_ONLY)
        val vertexStorageLevel = options.remove("vertexLevel")
                .map(StorageLevel.fromString).getOrElse(StorageLevel
                .MEMORY_ONLY)
        val fieldSeparator = options.remove("separator").getOrElse(":")
        val weighted = options.remove("weighted").exists(_ == "true")

        val objectiveH = options.remove("objectiveH").map(_.toDouble).getOrElse(0.5)
        val objectiveV = options.remove("objectiveV").map(_.toDouble).getOrElse(1.0)
        val objectiveD = options.remove("objectiveD").map(_.toDouble).getOrElse(0.8)
        val objectiveNorm = options.remove("objectiveNorm").map(_.toInt).getOrElse(2)
        partitionStrategy.setObjectiveParams(objectiveH, objectiveV, objectiveD, objectiveNorm)

        taskType match {
            case "load" => // loading test
                println("==========================")
                println("| Loading Test |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Loading Test (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)

                hypergraph.cache()

                println("==========================")
                println("HYPERX: Number of vertices: " +
                        hypergraph.vertices.count)
                println("HYPERX: Number of hyperedges: " +
                        hypergraph.hyperedges.count)
                println("==========================")

                sc.stop()

            case "part" =>
                println("==========================")
                println("| Partitioning |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Partitioning (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                val name = inputName(fname)
                hypergraph.hyperedges.saveAsObjectFile(outputPath + name + "/hyperedges")
                hypergraph.vertices.saveAsObjectFile(outputPath + name + "/vertices")
                sc.stop()

            case "tp" =>
                println("==========================")
                println("| Transition Probabilities |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Transition Probabilities (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                    .cache()

                val ret = TransitionProbabilities.run(hypergraph)
                ret.saveAsTextFile(outputPath + "tp")
                sc.stop()

            case "cc" =>
                println("==========================")
                println("| Clustering Coefficients |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Clustering Coefficients (" +
                        fname + ")"))

                sc.getConf.set("hyperx.debug.k", numPart.toString)

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                        .cache()

                val ret = ClusterCoefficients.run(hypergraph)
                ret.saveAsTextFile(outputPath + "cc")
                sc.stop()

            case "rw" =>
                println("==========================")
                println("| Random Walks |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Random Walks (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                    .cache()

                val maxIter = options.remove("maxIter").map(_.toInt).getOrElse(10)
                val num= options.remove("numStartVertices").map(_.toInt)
                        .getOrElse(hypergraph.numVertices.toInt / 1000)

                val ret = RandomWalk.run(hypergraph, num, maxIter)
                ret.vertices.saveAsTextFile(outputPath + "rw")

                sc.stop()

            case "lp" =>
                println("==========================")
                println("| Label Propagation |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Label Propagation (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                    .cache()

                val maxIter = options.remove("maxIter").map(_.toInt).getOrElse(100)
                val ret = LabelPropagation.run(hypergraph, maxIter)
                ret.vertices.saveAsTextFile(outputPath + "lp")

                sc.stop()

            case "bc" =>
                println("==========================")
                println("| Betweenness Centrality |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Between Centrality (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                        .cache()

                val ret = BetweennessCentrality.run(hypergraph)
                ret.filter(v => v._2 > 0.0).saveAsTextFile(outputPath + "bc")
                sc.stop()

            case "graph" =>
                println("==========================")
                println("| Graph Test |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Graph Test (" +
                    fname + ")"))
                val graph = GraphLoader.edgeListFile(sc, fname,
                    canonicalOrientation = false, numPart,
                    storage.StorageLevel.MEMORY_ONLY, vertexStorageLevel).cache()

                val rw = PageRank.run(graph, 100)
                rw.vertices.saveAsTextFile(test_path + "rw_graph")

                println("==========================")
                println("GRAPHX: Number of vertices: " +
                        graph.vertices.count)
                println("GRAPHX: Number of edges: " +
                        graph.edges.count)
                println("==========================")

                sc.stop()
        }
    }

    private def loadHypergraph(sc: SparkContext, fname: String, vfname: String,
        fieldSeparator: String, weighted: Boolean, numPart: Int,
        inputMode: String, partitionStrategy: HeuristicPartition,
        hyperedgeLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        vertexLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Hypergraph[Int, Int] = {
        val hypergraph = inputMode match {
            case `listMode` =>
                HypergraphLoader.hyperedgeListFile(sc, fname,
                    fieldSeparator, weighted, numPart, partitionStrategy,
                    hyperedgeLevel, vertexLevel)
            case `objectMode` =>
                HypergraphLoader.hypergraphObjectFile(sc, vfname, fname, numPart,
                    hyperedgeLevel, vertexLevel)
            case `partitionedListMode` =>
                HypergraphLoader.partitionFile(sc, fname, numPart, fieldSeparator,
                    hyperedgeLevel, vertexLevel)
        }
        hypergraph
    }

    private def inputName(path: String): String = {
        path.split("/").last
    }

    private val test_path = "hdfs://master:9000/apps/hyperx/output/"

    private val listMode = "list"
    private val objectMode = "object"
    private val partitionedListMode = "plist"
}
