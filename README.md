# neo4j-gpu
This repository contains the source code of my master's thesis where I've accelerated graph pattern queries by querying data stored in Neo4j using the GPU and comparing the performance to Cypher-queries in Neo4j.

# Thesis report
The published thesis can be found [here](http://uu.diva-portal.org/smash/record.jsf?pid=diva2%3A1056415&dswid=-7116#sthash.K6R0JhDe.dpbs) and its full-text can be read [here.](http://uu.diva-portal.org/smash/get/diva2:1056415/FULLTEXT01.pdf)

## Abstract
Over the last decade the popularity of utilizing the parallel nature of the graphical processing unit in general purpose problems has grown a lot. Today GPUs are used in many different fields where one of them is the acceleration of database systems. Graph databases are a kind of database systems that have gained popularity in recent years. Such databases excel especially for data which is highly interconnected. Querying a graph database often requires finding subgraphs which structurally matches a query graph, i.e. isomorphic subgraphs. In this thesis a method for performing subgraph isomorphism queries named GPUGDA is proposed, extending previous work of GPU-accelerating subgraph isomorphism queries. The query performance of GPUGDA was evaluated and compared to the performance of storing the same graph in Neo4j and making queries in Cypher, the query language of Neo4j. The results show large speedups of up to 470x when the query graph is dense whilst performing slightly worse than Neo4j for sparse query graphs in larger databases.
