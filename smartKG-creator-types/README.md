# Compilation

To compile, go inside /smartKG-creator/hdt-cpp-molecules

## Clean code

make clean

## Compile
./autogen.sh
./configure
make

# Show statistics 

./hdt-cpp-molecules/libhdt/tools/getFamiliesEstimate dataset.hdt

## Statistics Parameters

-S : Activate the option to get only families with a presence of a minimum % of subjects. Recommended for very unstructured datasets (e.g. Dbpedia).
-P <percentage>: set up the % of subjects to limit families with a minimum of the given <percentage>. It requires to activate the -S option. If -S is activate and -P is not specified, the default value is 0.01
-L <percentage>: Setup the percentage for infrequent predicates in % occurrences (the more, the less partitions). Predicates with less % occurrences (over the total number of triples) than the given percentage will be discarded and not considered in the families. The default value is 0.01%.
-H <percentage>: Setup the percentage to cut massive predicates in % occurrences (the more, the more partitions). Predicates with more % ocurrences (over the total number of triples) than the given percentage will be discarded. The default value is 0.1%. 
-q: Activate quick estimation (do not perform grouping)

# Generate families

./hdt-cpp-molecules/libhdt/tools/getFamilies dataset.hdt

## Generation Parameters

-s prefix: Prefix for the splitted families (e.g. part_watdiv.10M_). Mandatory
-e <exportFile.json> Export metadata of families in <exportFile>.json
-S : Activate the option to get only families with a presence of a minimum % of subjects. Recommended for very unstructured datasets (e.g. Dbpedia).
-P <percentage>: set up the % of subjects to limit families with a minimum of the given <percentage>. It requires to activate the -S option. If -S is activate and -P is not specified, the default value is 0.01
-L <percentage>: Setup the percentage for infrequent predicates in % occurrences (the more, the less partitions). Predicates with less % occurrences (over the total number of triples) than the given percentage will be discarded and not considered in the families. The default value is 0.01%.
-H <percentage>: Setup the percentage to cut massive predicates in % occurrences (the more, the more partitions). Predicates with more % ocurrences (over the total number of triples) than the given percentage will be discarded. The default value is 0.1%. 
-i Include infrequent predicates (they are not included by default).

