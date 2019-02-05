#!/bin/bash
usage ()
{
  echo 'Usage : csv2neo4j <path to artist.csv> <path to relations.csv>'
  exit
}

if [ "$#" -ne 2 ]
then
  usage
fi

./neo4j-admin import --nodes:Artist=$1 --relationships:TOCO_CON=$2

