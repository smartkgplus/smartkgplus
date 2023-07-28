#!/bin/bash
# by Paul Colby (http://colby.id.au), no rights reserved ;)

output_dir=$1
experiment_name=$2
number_of_clients=$3
count_required=$4

prev_total=0
prev_idle=0
 
while true; do
  # Get the total CPU statistics, discarding the 'cpu ' prefix.
  ts=$(date +%s)
  cpu_times=($(sed -n 's/^cpu\s//p' /proc/stat))
  mem_used=$(awk '/^Mem/ {print $3}' <(free -m))
  idle_time=${cpu_times[3]} # Just the idle CPU time.
 
  # Calculate the total CPU time.
  total_time=0
  for cpu_time in ${cpu_times[@]}; do
    total_time=$(( $total_time + $cpu_time ))
  done
 
  # Calculate the CPU usage since we last checked.
  diff_idle=$(( $idle_time - $prev_idle ))
  diff_total=$(( $total_time - $prev_total ))
  cpu_utilization=$(( (1000 * ($diff_total - $diff_idle) / $diff_total) / 10 ))
  echo "$ts,$cpu_utilization,$mem_used" >> "${output_dir}/${experiment_name}_${number_of_clients}_stats.csv"
 
  # Remember the total and idle CPU times for the next check.
  prev_total=$total_time
  prev_idle=$idle_time
 
  # Wait before checking again.
  sleep 1
done

