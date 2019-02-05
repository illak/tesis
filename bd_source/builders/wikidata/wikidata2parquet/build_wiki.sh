#!/bin/bash

set -x

# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}



# Set dir for wikidata data
DB_TRANSFORMED=../../../../files/db

# Set input JSON dir
FIN="$DB_TRANSFORMED/extracted/wikidata/"
# Set output dir
FOUT="$DB_TRANSFORMED/transformed/tmp1/wikidata/"

LOGFILETIME="$(pwd)/log_time.txt"


(time $SPARK_HOME/bin/spark-submit \
	  --master local[*] \
	    --driver-memory 50g \
	      --packages com.databricks:spark-xml_2.10:0.4.0 \
	        target/scala-2.11/*.jar "$FIN" "$FOUT") 2>"$LOGFILETIME"

#  --packages com.databricks:spark-xml_2.10:0.4.0 \

echo $?

