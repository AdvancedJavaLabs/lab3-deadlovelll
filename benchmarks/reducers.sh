#!/usr/bin/env bash
set -e

JOB_JAR=/tmp/job.jar
MAIN_CLASS=sales.SalesDriver
INPUT=/sales_input
OUTPUT=/sales_output

SPLIT=134217728
SPLIT_MB=128

RESULT_FILE=benchmark_reducers_results.csv
echo "reducers,split_mb,real_time_sec" > "$RESULT_FILE"

REDUCERS=(
  1
  2
  4
  8
  16
)

for R in "${REDUCERS[@]}"; do
  echo
  echo "===================================="
  echo "Running benchmark: reducers=${R}, split=${SPLIT_MB}MB"
  echo "===================================="

  hdfs dfs -rm -r -f "$OUTPUT" >/dev/null 2>&1 || true

  START=$(date +%s)

  hadoop jar "$JOB_JAR" "$MAIN_CLASS" \
    -Dmapreduce.input.fileinputformat.split.maxsize="$SPLIT" \
    -Dmapreduce.input.fileinputformat.split.minsize="$SPLIT" \
    -Dmapreduce.job.reduces="$R" \
    -Dmapreduce.map.memory.mb=2048 \
    -Dmapreduce.reduce.memory.mb=2048 \
    -Dmapreduce.map.java.opts=-Xmx1536m \
    -Dmapreduce.reduce.java.opts=-Xmx1536m \
    "$INPUT" "$OUTPUT"

  END=$(date +%s)
  REAL_TIME=$((END - START))

  echo "Reducers ${R} -> time ${REAL_TIME}s"
  echo "${R},${SPLIT_MB},${REAL_TIME}" >> "$RESULT_FILE"
done

echo
echo "Benchmark finished"
echo "Results saved to $RESULT_FILE"
