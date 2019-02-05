#!/bin/bash

set -x

# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}

DB_DIR=../../../../files/db
# Set discogs releases data path
FINREL="$DB_DIR/extracted/discogs/discogs_20180901_releases.xml"
# Set discogs artists data path
FINART="$DB_DIR/extracted/discogs/discogs_20180901_artists.xml"
# Set output dir
FOUT="$DB_DIR/transformed/tmp1/discogs"

LOGFILETIME="$(pwd)/log_time.txt"

(time $SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --driver-memory 50g \
  --packages com.databricks:spark-xml_2.11:0.4.1 \
  target/scala-2.11/*.jar \
  "$FINREL" "$FINART" "$FOUT") 2>"$LOGFILETIME"

echo $?
