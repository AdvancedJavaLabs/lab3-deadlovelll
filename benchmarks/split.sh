#!/usr/bin/env bash
set -e

JOB_JAR=/tmp/job.jar
MAIN_CLASS=sales.SalesDriver
INPUT=/sales_input
OUTPUT=/sales_output

RESULT_FILE=benchmark_split_results.csv
echo "split_bytes,split_mb,real_time_sec" > "$RESULT_FILE"

SPLITS=(
  134217728
  67108864
  33554432
  16777216
  8388608
  4194304
)

for SPLIT in "${SPLITS[@]}"; do
  SPLIT_MB=$((SPLIT / 1024 / 1024))

  echo
  echo "===================================="
  echo "Running benchmark: split=${SPLIT_MB}MB"
  echo "===================================="

  hdfs dfs -rm -r -f "$OUTPUT" >/dev/null 2>&1 || true

  START=$(date +%s)

  hadoop jar "$JOB_JAR" "$MAIN_CLASS" \
    -Dmapreduce.input.fileinputformat.split.maxsize="$SPLIT" \
    -Dmapreduce.input.fileinputformat.split.minsize="$SPLIT" \
    -Dmapreduce.job.reduces=1 \
    "$INPUT" "$OUTPUT"

  END=$(date +%s)
  REAL_TIME=$((END - START))

  echo "Split ${SPLIT_MB}MB -> time ${REAL_TIME}s"
  echo "${SPLIT},${SPLIT_MB},${REAL_TIME}" >> "$RESULT_FILE"
done

echo
echo "Benchmark finished"
echo "Results saved to $RESULT_FILE"
