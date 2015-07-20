HyperX
======

A scalable framework for hypergraph processing and learning algorithms. HyperX is built upon Apache Spark and inspired by its graph counterpart, GraphX.

When processing a hypergraph (where an edge contains arbitrary number of vertices), instead of converting the hypergraph to a bipartite and employing GraphX to do the tricks, HyperX directly operates on a distributed hypergraph representation. By carefully optimizing the hypergraph partitioning strategies, the preliminary exprimental results show that HyperX is able to achieve a 49 speedup factor on the hypergraph random walks upon the bipartite GraphX solution.

A paper describing the details is now under review for ICDM 2015. A technical report can be found at  http://iojin.com/resources/hyperx_report.pdf.


