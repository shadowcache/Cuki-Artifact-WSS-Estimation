#!/bin/bash
set +ex

JAVA="java"
JAR="./target/working-set-size-estimation-1.0-SNAPSHOT-jar-with-dependencies.jar"
CLASS_NAME="alluxio.client.file.cache.benchmark.BenchmarkMain"

to_brief_string() {
  local size=$1
  if [[ ${size} -ge "1048576" ]]; then
    echo "$(( size / 1048576 ))m"
  elif [[ ${size} -ge "1024" ]]; then
    echo "$(( size / 1024 ))k"
  else
    echo "${size}k"
  fi
}


to_brief_string_time() {
  local size=$1
  echo "$(( size / 3600000 ))h"
}

bench_one() {
    mkdir -p ./datasets/benchmarks/mrc-acc/rarcm
  ${JAVA} -cp ${JAR} ${CLASS_NAME} \
    --benchmark "mrc_accuracy" \
    --shadow_cache "rar" \
    --max_entries "262144" \
    --window_size "262144" \
    --report_interval "1000" \
    --num_hash "8" \
    --memory ${memory} \
    --size_bits "32" \
    --clock_bits "6"  \
    --scope_bits "0" \
    --rd_length ${rd_length} \
    --rd_width ${rd_width} \
    --opportunistic_aging "false" \
    --dataset ${dataset} \
    --trace ${trace} \
    > "./datasets/benchmarks/mrc-acc/rarcm/${dataset}.log"
  echo "./datasets/benchmarks/mrc-acc/rarcm/${dataset}.log"
} 


# memory="10mb"
# rd_length=1024
# rd_width=1048576 # 1mb
# dataset="msr"
# trace="./datasets/msr/prxy_0.csv"
# bench_one

# memory="10mb"
# rd_length=2048
# rd_width=1048576 # 1mb
# dataset="twitter"
# trace="./datasets/twitter/cluster37-1h-new.csv"
# bench_one

memory="10mb"
rd_length=4096
rd_width=16777216 # 16mb
dataset="ycsb"
trace="./datasets/ycsb/ycsb-1m-10m-1m-concat6.csv"
bench_one
echo "test rarcm end."