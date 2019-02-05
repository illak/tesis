
#!/bin/bash

set -x



# Set Spark binaries dir
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}

BD_FILES=../../files/db

YEAR=${YEAR:-2018}

# Set input dir of computed paths
FINPUTDIR1="$BD_FILES/graph/output/path_computing/${YEAR}/pqt"
# Set input dir of CSV with guest artists
FINPUTDIR2="$BD_FILES/graph/csv/${YEAR}"
# Set input dir of CSV with relevant artists
FINPUTDIR3="$BD_FILES/graph/csv/relevants"
# Set output dir
FOUTDIR="$BD_FILES/graph/output/path_ranking/${YEAR}"

LOGFILETIME="$(pwd)/log_time.txt"

if [ ! -d "${FINPUTDIR1%/}/" ]; then

    printf '\nInput parquet dir:%s not found. Please check that it exists!' "${FINPUTDIR1%/}/"
    exit 0
fi


if [ ! -d "${FOUTDIR%/}/path_filter.pqt" ]; then

    (time $SPARK_HOME/bin/spark-submit \
      --master local[*] \
      --driver-memory 50g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      target/scala-2.11/*.jar \
      "$FINPUTDIR1" "$FINPUTDIR2" "$FINPUTDIR3" "$FOUTDIR") 2>"$LOGFILETIME"
else
    printf '\nParquet already exists at %s remove path_filter.pqt dir before running this program!.\n\n' "$FOUTDIR"
fi

echo $?
