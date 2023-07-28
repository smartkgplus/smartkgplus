#!/bin/bash

ip=$1
user=$2
pass=$3
file=$4
dataset=$5
method=$6
queries=$7
out=$8

num=1
while [ $num -lt 129 ]
do
  bash ipset.sh ${num}
  ./track_cpu_mem.sh cpu_${method}_${dataset} ${method} ${num} 1 &
  echo sshpass -p [PASSWORD] ssh $user@$ip \"cd Cln/ \&\& ./run_clients.sh ${method} $num $dataset\" 
  sshpass -p $pass ssh $user@$ip "cd Cln/ && ./run_clients.sh ${method} $num $dataset $queries $out" > out_${method}_$num & pids+=($!)
  echo "Waiting for ${method} with dataset $dataset and $num clients"
  wait "${pids[@]}"
  ./stop_tracking.sh

  num=$[$num*2]
done
