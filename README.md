# smart-KG: Partition-Based Linked Data Fragments for Querying Knowledge Graphs

# Table of contents
1. [Abstract](#Abstract)
2. [Client and Server implementation](#Implementation)
3. [Typed-Family Partitioning](#typed)
4. [Query Workloads](#QueryWorkloads)
5. [Experiments](#Experiments)
6. [DBpedia Analysis](#DBpedia)


## Abstract <a name="Abstract"></a>
RDF and SPARQL provide a uniform way to publish and query billions of triples in open knowledge graphs (KGs) on the Web. Yet, provisioning of a fast, reliable, and responsive live querying solution for open KGs is still hardly possible through SPARQL endpoints alone: while such endpoints provide a remarkable performance for single queries, they typically can not cope with highly concurrent query workloads by multiple clients. To mitigate this, the Linked Data Fragments (LDF) framework sparked the design of different alternative low-cost interfaces such as Triple Pattern Fragments (TPF), that partially offload the query processing workload to the client side. On the downside, such interfaces come with the expense of higher network load due to the necessary transfer of intermediate results to the client, also leading to query performance degradation compared with endpoints. To address this problem, in this work, we investigate alternative interfaces able to ship partitions of KGs from the server to the client, which aim at reducing server-resource consumption. To this extent, first, we align formal definitions and notations of the original LDF framework to uniformly present partition-based LDF approaches. These novel LDF interfaces retrieve, instead of the exact triples matching a particular query pattern, a subset of partitions from materialized, compressed graph partitions to be further evaluated on the client side. Then, we present smart-KG, a concrete partition-based LDF approach. Our proposed approach is a step forward towards a better-balanced share of query processing load between clients and servers by shipping graph partitions driven by the structure of RDF graphs to group entities described with the same sets of properties and classes, resulting in significant data transfer reduction. Our experiments demonstrate that smart-KG significantly outperforms existing Web SPARQL interfaces on both pre-existing benchmarks for highly concurrent query execution and a novel query workload benchmark we introduce – inspired by query logs of existing SPARQL endpoints.

## Client and Server implementation <a name="Implementation"></a>
The client and server implementation of smartKG+ is based on WiseKG implementation introduced here: https://github.com/WiseKG/WiseKG-Java. In fact, in this implementation, we applied all of the findings on WiseKG. Since WiseKG is combining SPF and smartKG, we in this code have replaced smartKG with smartKG+.
For more details on the the three configurations mentioned in the ablation study: please have a look README at smartKG+-2023 folder


## Typed-Family Partitioning <a name="typed"></a>
This code generates typed-family partitions as defined in the paper. If you would like more details on the installation and how to use it. Please have a look README at smartKG-creator-types folder

## Query Workloads <a name="QueryWorkloads"></a>

This folder contains all the query workloads that have been described in Table 4 in the paper.

## Experiments <a name="Experiments"></a>

This folder contains all the log files for the experiments that have been performed in the papers. Some experiments have been reused from the WiseKG paper to avoid rerunning the servers on the same software, the same datasets, and the same hardware configurations. Also for more extensive details about the arichtecture used and the docker version you can use the following two recources:
1. Github repistory that have a docker image of the entire experiments: https://github.com/AmrTAzzam/WebQuerying-Experiments
2. Chapter 8 in the PhD thesis: https://aic.ai.wu.ac.at/~polleres/supervised_theses/Amr_Azzam_PhD_2023.pdf

## DBpedia Analysis <a name="DBpedia"></a>

It contains a detailed analysis on DBpedia predicates and classes and how we selected the predicates and classes to be materialized. For more details, please have a look README at DBpedia Analysis folder
