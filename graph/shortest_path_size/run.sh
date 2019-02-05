#!/bin/bash

set -x


YEAR=2016

case $USER in
    izapata)
        FILESHOME=$HOME/files
        FOUTDIR="/home/izapata/parquets/shortest_path_testing/$YEAR"
        ;;
    damian)
        FILESHOME=$HOME/space/illak/files
        FOUTDIR="$FILESHOME/tmp/shortest_path_testing/$YEAR"
esac

FINPUTDIR="$FILESHOME/db/graph/degree_CC_CI_CII"
FINPUTDIR1="$FILESHOME/db/graph/csv/$YEAR"
FINPUTDIR2="$FILESHOME/db/graph/csv/relevants"
NUMREL=1
LOGFILETIME="$(pwd)/log_time.txt"

SPARK_HOME=$HOME/spark/spark-2.3.0-bin-hadoop2.7

(time $SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --driver-memory 50g \
  --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
  target/scala-2.11/*.jar \
  "$FINPUTDIR" "$FINPUTDIR1" "$FINPUTDIR2" "$NUMREL" "$FOUTDIR") 2>"$LOGFILETIME"

echo $?
