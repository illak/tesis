#!/bin/bash

set -x


# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}

# Set dir for db files
DB_FILES=../../files/db

# Set year
YEAR=${YEAR:-2018}

# Set input dir for ranked paths
FINPUTDIR1="$DB_FILES/graph/output/path_ranking/${YEAR}/path_filter.pqt"
# Set max ranking
RANK=5

# Set output dir
FOUTDIR="$DB_FILES/graph/output/ids/${YEAR}"


LOGFILETIME="$(pwd)/log_time.txt"




printf 'running program...'
(time $SPARK_HOME/bin/spark-submit \
      --master local[*] \
      --driver-memory 50g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      target/scala-2.11/*.jar \
      "$FINPUTDIR1" "$RANK" "$YEAR" "$FOUTDIR") 2>"$LOGFILETIME"



echo $?
