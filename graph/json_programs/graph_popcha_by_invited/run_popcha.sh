#!/bin/bash

set -x

# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}

# Set Year
YEAR=${YEAR:-2018}

# Set dir for db files
DB_FILES=../../../files/db

# Set input dir of paths with ranking
FINPUTDIR1="$DB_FILES/graph/output/path_ranking/$YEAR"
# Set input dir of CSV with guest artists
FINPUTDIR2="$DB_FILES/graph/csv/$YEAR"
# Set input dir of CSV with relevant artists
FINPUTDIR3="$DB_FILES/graph/csv/relevants"
# Set input dir of paths with ranking
FINPUTDIR4="$DB_FILES/transformed"
# Set max Rank value
RANK=5
# Set output dir
FOUTDIR="$DB_FILES/graph/output/viz/popcha_by_invited/$YEAR"

LOGFILETIME="$(pwd)/log_time.txt"


printf 'running program...'
(time $SPARK_HOME/bin/spark-submit \
      --master local[*] \
      --driver-memory 5g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      target/scala-2.11/*.jar \
      "$FINPUTDIR1" "$FINPUTDIR2" "$FINPUTDIR3" "$FINPUTDIR4" "$RANK" "$YEAR" "$FOUTDIR") 2>"$LOGFILETIME"



echo $?
