#!/bin/bash

# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

PARENTDIR=$(dirname `pwd`)

JARFILE=$PARENTDIR/spark_programs/searchMusicbrainz/target/scala-2.11/*.jar

SPARK_HOME=$1

# AGREGAR CONFIG PARA SPARK DE SER NECESARIO (e.g +memoria)
$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  $JARFILE ${@:2}

echo $?
