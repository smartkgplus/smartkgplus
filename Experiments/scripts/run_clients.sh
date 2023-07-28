#!/bin/bash

file=$1
method=$2
num=$3
dataset=$4
queries=$5
out=$6

old_num=$num
num="$(($num-1))"

./track_cpu_mem.sh ${out}/cpu ${method}_${load} ${old_num} 1 &
for i in $(seq 0 1 $num)
do
  java -jar $file config.json$ ${queries} ${method} ${out} ${old_num} $i ${dataset} > out_${method}_${dataset}_$i & pids[$i]=$!
done

echo "Waiting..."
wait "${pids[@]}"

./stop_tracking.sh
