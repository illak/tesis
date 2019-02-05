#!/bin/bash

# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}

# Set dir for sources parquets
DB_TRANSFORMED=../../files/db


# Set input dir
FIN="$DB_TRANSFORMED/transformed/tmp1"
# Set output dir
FOUT="$DB_TRANSFORMED/transformed/"

LOGFILETIME="$(pwd)/log_time.txt"


(time $SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --driver-memory 50g \
  --executor-memory 50g \
  target/scala-2.11/*.jar "$FIN" "$FOUT") 2>"$LOGFILETIME"


echo $?
