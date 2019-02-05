#!/bin/bash

SPARK_HOME=/home/damian/spark/spark-2.0.2-bin-hadoop2.7

FIN="/home/izapata/files/discogs/discogs_20170401_releases.xml"
FOUT="/home/damian/space/discogs/discogs_releases.pqt"


time $SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --driver-memory 50g \
  target/scala-2.11/*.jar "release" "$FIN" "$FOUT"

#  --packages com.databricks:spark-xml_2.10:0.4.1 \

echo $?
