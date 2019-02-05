#!/bin/bash

set -x

# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}

# Set dir for db files
DB_FILES=../../files/db

# Set directory of unified DB
FINDIR="$DB_FILES/transformed/final"
# Set directory of relevants CSV file
FINDIR2="$DB_FILES/graph/csv/relevants"
# Set output dir"
FOUTDIR="$DB_FILES/graph/degree_CC_CI_CII"

LOGFILETIME="$(pwd)/log_time.txt"

(time $SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --driver-memory 50g \
  --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
  target/scala-2.11/*.jar \
  "$FINDIR" "$FINDIR2" "$FOUTDIR") 2>"$LOGFILETIME"

echo $?
