# Compilation

To compile, go inside /smartKG-creator/hdt-cpp-molecules

## Clean code

make clean

## Compile
./autogen.sh
./configure
make

# Show statistics 

We show basic statistics on the input dataset such as the total number
of triples, unique predicates, initial families, and merged families using the following
command:

./hdt-cpp-molecules/libhdt/tools/getFamiliesEstimate [arguments] <hdtfile>

## Statistics Parameters

-S: This parameter enables the selection of only those families that have a
minimum percentage of subjects present in the dataset. This option is particularly useful for unstructured datasets like Dbpedia. By default, the minimum
percentage is set to αs = 0.01. To modify this value, users need to invoke the "-P" parameter.

-P: This parameter allows users to set the minimum percentage of subjects required for a family to be selected. This parameter can only be used in conjunction with the "-S" parameter. If the "-S" parameter is enabled and "-P" is not specified, the default value of 0.01 is used. The value specified for this parameter denotes αs in equation (23).


-L <percentage>: This parameter allows users to specify the percentage of infrequent predicates in terms of their occurrences within the dataset. Predicates that have less than the specified percentage of occurrences (as a percentage of the total number of triples) will be discarded and not considered in the families. The default value of this parameter is 0.01%. The value specified for this parameter corresponds to τplow in equation (21).

-H <percentage>: This parameter allows users to specify the percentage of occurrences at which massive predicates are to be cut, expressed as a percentage of the total number of triples. Predicates that have more than the specified percentage of occurrences will be discarded. The default value of this parameter is 0.1%. The value specified for this parameter corresponds to τphigh in equation (21).

-m <Percentage>: This parameter enables users to specify the maximum size of a new group in terms of a percentage of the total number of triples. For instance, if the parameter is set to 5, a new group is created only if the estimated size is less than 5% of the total number of triples. If "-m" is set to 100, then all groups are allowed. The value of this parameter corresponds to αt in equation (24).

-q: activate quick estimation (do not perform grouping)

# Generate families

./hdt-cpp-molecules/libhdt/tools/getFamilies [arguments] <hdtfile>

## Generation Parameters

-s <splitFilePrefix>: This argument allows users to partition triples based on existing families that are described in the specified JSON file with the given prefix.

-e <exportFile>: This argument exports the metadata of families in <exportFile>.json and the groups in <exportFile>_group.json. This information can be used by the query planner to locate the HDT partition containing the results of a given query.

-i: This argument includes infrequent predicates with occurrences less than the user-defined threshold τl (default 0.01%), which may result in the creation of more partitions. This argument is set to false by default.

-C <classesFile>: this argument accepts a file containing a list of classes separated by a new line. The typed-family partition is applied only to the classes listed in this file. This argument is used only to perform typed-family partitioning as defined in the paper.

-d: This argument dumps infrequent predicates into a dedicated JSON file with the prefix "_infreqPreds". Infrequent predicates are defined by Equation (21).

-u: ungroup – This argument performs family partitioning without the grouping step, which generates partitions based solely on the original families defined in
Equations 4.2 and 4.3.

-G: This argument exports each family into a separate JSON file.

-v: This argument enables verbose mode, providing detailed results by printing all triples during partitioning. We recommend using this argument only for testing purposes.

-h: This argument provides a verbose explanation of the available arguments.

# How to generate the partitions used in the experiments:



