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

We have developed smartkgplus based on WiseKG implementation and in the following, we will show how can you reproduce the three aforementioned configurations:
