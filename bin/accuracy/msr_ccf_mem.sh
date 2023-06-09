#!/bin/bash
set -ex

source bin/bench_one.sh

JAVA="java"
JAR="./target/working-set-size-estimation-1.0-SNAPSHOT-jar-with-dependencies.jar"
CLASS_NAME="alluxio.client.file.cache.benchmark.BenchmarkMain"
BENCHMARK="accuracy"
DATASET="msr" # optional: msr, twitter, random, sequential
SHADOW_CACHE="ccf" # optional: ccf, mbf
TRACE="./datasets/msr/prxy_0.csv" # msr
MAX_ENTRIES=12582912 # 12m
WINDOW_SIZE=262144 # 256k
NUM_UNIQUE_ENTRIES=262144 # used for random & sequential benchmark
REPORT_DIR="./datasets/benchmarks/memory-ccf"
REPORT_INTERVAL=64
SIZE_BITS=20
NUM_SCOPE=0
SCOPE_BITS=0
TIME_DIVISOR=1

# switch this value to disable/enable opportunistic aging
OPPO_AGING=false;
LOCK_NUMBER=1024;

TAGS_PER_BUCKET=4; TAG_BITS=8;
SIZE_ENCODE="BUCKET"; SIZE_BITS=1; SIZE_BUCKET_BITS=0;
CLOCK_BITS=1;
WINDOW_ELEMENTS=32768

for BITS in `seq 1 1 8`; do
  CLOCK_BITS=${BITS}
  SIZE_BITS=${BITS}
  SIZE_BUCKET_BITS=$(( 16 - SIZE_BITS ))
  TOTAL_BYTES=$(( (TAG_BITS + SIZE_BITS + SCOPE_BITS + CLOCK_BITS) * WINDOW_ELEMENTS / 8 ))
  MEMORY="$(to_brief_string ${TOTAL_BYTES})b"
  echo "${TOTAL_BYTES}"
  #echo "${MEMORY}-${SIZE_ENCODE}_${SIZE_BITS}_${SIZE_BUCKET_BITS}-c${CLOCK_BITS}"
  bench_one_ccf
done

MEMORY="192kb"
bench_one_ccf


bash bin/parse-log.sh \
  `ls -tr ${REPORT_DIR}/${BENCHMARK}/${DATASET}/*_${OPPO_AGING}.log` \
  >  ${REPORT_DIR}/${BENCHMARK}/${DATASET}/summary_${OPPO_AGING}.csv

echo ${REPORT_DIR}/${BENCHMARK}/${DATASET}/summary_${OPPO_AGING}.csv