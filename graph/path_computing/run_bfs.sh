#!/bin/bash

set -x


# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}


# Set dir for db files
DB_FILES=../../files/db

YEAR=${YEAR:-2018}

# Set input dir of generated graph
FINPUTDIR1="$DB_FILES/graph/degree_CC_CI_CII"
# Set input dir of CSV with guest artists
FINPUTDIR2="$DB_FILES/graph/csv/$YEAR"
# Set input dir of CSV with relevant artists
FINPUTDIR3="$DB_FILES/graph/csv/relevants"
# Set min number of releases in common
NUMREL="2"
# Set output dir
FOUTDIR="$DB_FILES/graph/output/path_computing/$YEAR"

LOGFILETIME="$(pwd)/log_time.txt"

if [ ! -d "${FOUTDIR%/}/pqt" ]; then

    printf 'running program...'
    (time $SPARK_HOME/bin/spark-submit \
      --master local[*] \
      --driver-memory 50g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      target/scala-2.11/*.jar \
      "$FINPUTDIR1" "$FINPUTDIR2" "$FINPUTDIR3" "$NUMREL" "$FOUTDIR") 2>"$LOGFILETIME"

else
    printf '\nParquet already exists at %s remove pqt/ dir before running this program!.\n\n' "$FOUTDIR"
fi

echo $?
