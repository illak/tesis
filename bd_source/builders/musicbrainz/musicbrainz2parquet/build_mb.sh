#!/bin/bash

set -x

# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}

# Set dir for musicbrainz data
DB_DIR=../../../../files/db


FIN="$DB_DIR/extracted/musicbrainz/mbdump/"
# Set output dir
FOUT="$DB_DIR/transformed/tmp1/musicbrainz/"

[ -d "$FOUT" ] || mkdir "$FOUT"

LOGFILETIME="$(pwd)/log_time.txt"

(time $SPARK_HOME/bin/spark-submit \
            --master local[*] \
            --driver-memory 50g \
            --class Main \
	        target/scala-2.11/*.jar "$FIN" "$FOUT") 2> "$LOGFILETIME"


echo $?

