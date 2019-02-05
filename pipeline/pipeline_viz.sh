#!/bin/bash

set -x


INPUTYEAR=$1


# SET 2x NUMBER OF CORES AVAILABLES
CORES=12

# SET FILES DIRECTORY HERE
FILESHOME=$HOME/files

# SET SPARK DIRECTORY HERE
SPARK_HOME=$HOME/spark/spark-2.3.1-bin-hadoop2.7

# OPTIONAL VIRTUALENV PYTHON DIR
VIRTUALENV=$HOME/python_env/test

PROGRAMSDIR=../graph

GUESTSCSV="$FILESHOME/db/graph/csv/$INPUTYEAR"
RELEVANTSCSV="$FILESHOME/db/graph/csv/relevants"

# DEFAULT SETTINGS
RANK=5
NUMREL=2



# CREATE LOGS folder if dont exists
mkdir -p LOGS

#===========================PATH TESTING==============================

FINPUTDIRP1="$FILESHOME/db/graph/degree_CC_CI_CII"

FOUTDIRP1="$FILESHOME/db/graph/path_testing/$INPUTYEAR"

LOGFILETIME1="$(pwd)/LOGS/path_testing_log_time.txt"

if [ ! -d "$FOUTDIRP1/pqt/" ]; then

    printf 'running program: PATH TESTING...\n'
    (time $SPARK_HOME/bin/spark-submit \
      --master local[$CORES] \
      --driver-memory 50g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      $PROGRAMSDIR/path_testing/target/scala-2.11/*.jar \
      "$FINPUTDIRP1" "$GUESTSCSV" "$RELEVANTSCSV" "$NUMREL" "$FOUTDIRP1") 2>"$LOGFILETIME1"

else
    printf '\nParquet already exists at %s remove pqt dir before running this program!.\n\n' "$FOUTDIRP1"
fi &&

#===========================PATH RANKING==============================

FOUTDIRP2="$FILESHOME/db/graph/output/path_filter_output/$INPUTYEAR"
LOGFILETIME2="$(pwd)/LOGS/path_filter_log_time.txt"

if [ ! -d "$FOUTDIR2path_filter.pqt" ]; then

    printf 'running program: PATH RANKING ...\n'
    (time $SPARK_HOME/bin/spark-submit \
      --master local[$CORES] \
      --driver-memory 50g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      $PROGRAMSDIR/path_ranking/target/scala-2.11/*.jar \
      "$FOUTDIRP1" "$GUESTSCSV" "$RELEVANTSCSV" "$FOUTDIRP2") 2>"$LOGFILETIME2"
else
    printf '\nParquet already exists at %s remove path_filter.pqt dir before running this program!.\n\n' "$FOUTDIRP2"
fi &&

#========================== ONLY IDS ==================================

FOUTDIRIDS="$FILESHOME/db/graph/output/ids/$INPUTYEAR"
LOGFILETIMEIDS="$(pwd)/LOGS/only_ids_log_time.txt"

printf 'running program: ONLY IDS ...\n'
    (time $SPARK_HOME/bin/spark-submit \
      --master local[$CORES] \
      --driver-memory 50g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      $PROGRAMSDIR/graph_only_ids/target/scala-2.11/*.jar \
      "$FOUTDIRP2/path_filter.pqt" "$RANK" "$INPUTYEAR" "$FOUTDIRIDS") 2>"$LOGFILETIMEIDS" &&

#=========================== IMAGES FETCHER ===========================

OUTPUT_DIR_IMAGES="$FILESHOME/db/graph/output/images_from_discogs/$INPUTYEAR"

printf 'running program: IMAGES FETCHER ...\n'

if [ -z "$VIRTUALENV" ]
then
    python discogs_api_images.py "$FOUTDIRIDS" "$INPUTYEAR" -o "$OUTPUT_DIR_IMAGES"
else
    source $VIRTUALENV/bin/activate
    python discogs_api_images.py "$FOUTDIRIDS" "$INPUTYEAR" -o "$OUTPUT_DIR_IMAGES"
    deactivate &&
fi

#=========================== GRAPH POPCHA ==============================

FINPUTDIRP3="$FOUTDIRP2/path_filter.pqt"
FOUTDIRP3="$FILESHOME/db/graph/output/popcha/$INPUTYEAR"

LOGFILETIME3="$(pwd)/LOGS/graph_popcha_log_time.txt"
SPARK_HOME=$HOME/spark/spark-2.2.1-bin-hadoop2.7

printf 'running program: GRAPH POPCHA ...\n'
(time $SPARK_HOME/bin/spark-submit \
      --master local[$CORES] \
      --driver-memory 50g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      $PROGRAMSDIR/json_programs/graph_popcha/target/scala-2.11/*.jar \
      "$FINPUTDIRP3" "$GUESTSCSV" "$RELEVANTSCSV" "$RANK" "$INPUTYEAR" "$OUTPUT_DIR_IMAGES" "$FOUTDIRP3") 2>"$LOGFILETIME3"

#=========================== GRAPH POPCHA INDIVIDUAL ==============================

FOUTDIRP4="$FILESHOME/db/graph/output/popcha_single/$INPUTYEAR"

LOGFILETIME4="$(pwd)/LOGS/graph_popcha_individual_log_time.txt"

printf 'running program: GRAPH POPCHA INDIVIDUAL ...\n'
(time $SPARK_HOME/bin/spark-submit \
      --master local[$CORES] \
      --driver-memory 50g \
      --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
      $PROGRAMSDIR/json_programs/graph_popcha_individual/target/scala-2.11/*.jar \
      "$FINPUTDIRP3" "$GUESTSCSV" "$RELEVANTSCSV" "$RANK" "$INPUTYEAR" "$OUTPUT_DIR_IMAGES" "$FOUTDIRP4") 2>"$LOGFILETIME4"

#=========================== SPOTIFY TREE ==============================

FOUTDIRP5="$FILESHOME/db/graph/output/tree_spotify/$INPUTYEAR"

LOGFILETIME5="$(pwd)/LOGS/spotify_log_time.txt"

printf 'running program: SPOTIFY TREE ...\n'
(time $SPARK_HOME/bin/spark-submit \
  --master local[$CORES] \
  --driver-memory 50g \
  --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
  $PROGRAMSDIR/json_programs/graph_path_tree/target/scala-2.11/*.jar \
  "$FINPUTDIRP3" "$GUESTSCSV" "$RELEVANTSCSV" "$INPUTYEAR" "$OUTPUT_DIR_IMAGES" "$FOUTDIRP4") 2>"$LOGFILETIME5"


echo $?
