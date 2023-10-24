# smartkgplus Java Implementation
A GitHub repository for the Java implementation of the smartkgplus client and server.

This repository includes two directories, (i) smartkgplus.Client which contains the implementation of the WiseKG client, and (ii) smartkgplus.Server which contains the implementation of the WiseKG server.
For more info on running and installing smartkgplus, please visit those subdirectories.


## Experiments Configurations:

We conduct an ablation study to evaluate the performance of each individual contribution made to smart-KG+. For this purpose, we developed three configurations of the interface:

1)  TPF+OP: This configuration represents the early version of smart-KG, combining TPF with client-side
query planning (OP).
2) TPF+NP: This configuration is a variant of our smartKG interface that allows us to observe the impact of the
new server-side query planning (NP) while using TPF.
3) brTPF+NP: This configuration represents our proposed solution smart-KG+, which combines brTPF with
server-side query planning (NP)

We have developed smartkgplus based on WiseKG implementation. First, on creating an execution plan from WiseKG.server, we do not use star pattern query processing on the servers. We always use partition shipping and we do that by updating QueryExecutionPlanServlet.java where put a very high value on executing the query locally on the server side (line 347) so that we always ship a partition to the client-side. Second, we decide if we use family partitions or typed-family partitions line (320 - 327).

In the following, we will show how can you reproduce the three aforementioned configurations:

1)  TPF+OP: This is our first implementation of smartKG. You can find the implementation in this code repository: https://git.ai.wu.ac.at/beno/smartkg.

2-3)  TPF+NP and brTPF+NP - The WiseKG implementation supports both TPF and brTPF and we can use either of the implementations by changing the request. In the following, we refer to this part of the implementation:
    - On the server-side, we have implemented LinkedDataFragmentServlet.java to receive the request and decide whether it is a TPF or brTPF according to request.getParameter("triples") which checks if there are bindings in the request or not (line 265 - line 285).
  
    - On the client-side, we have implemented SparqlQueryProcessor.java which has two constructors with a boolean tpf. This boolean allows us to switch between tpf and brTPF. In Experiments.java, we switch the boolean tpf based on the configuration we want to evaluate TPF+NP and brTPF+NP. In WiseKGHttpRequestTask.java, we constructURLSingle either by attaching the bindings in case of brTPF request or without bindings in the case of TPF. When the client receives a response from the server and it will be parsed using StarHandlerSingle.java which can differentiate between TPF and brTPF responses. 

