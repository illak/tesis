#!/bin/bash

# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

# Set Spark Home
# example --> SPARK_HOME=/home/illak/spark/spark-2.3.1-bin-hadoop2.7
SPARK_HOME=${SPARK_HOME:-$HOME/spark/spark-2.3.1-bin-hadoop2.7}

# Set tmp2 dir
# example --> TMP2_DIR=/home/illak/tesis/app_artists_finder/tmp2
TMP2_DIR=${TMP2_DIR:-../../files/db/transformed/tmp2}

# Set BD Target dir
# example --> BDTARGET_DIR=/home/illak/tesis/app_artists_finder/final
BDTARGET_DIR=${BDTARGET_DIR:-../../files/db/transformed/final}

# Set BD Target updated dir
# example --> BDTARGET_UPDATED_DIR/home/illak/tesis/app_artists_finder/final_updated
BDTARGET_UPDATED_DIR=${BDTARGET_UPDATED_DIR:-../../files/db/transformed/final_updated}

# Set BD Target update (1=activated, 0=deactivated)
BDTARGET_UPDATE=${BDTARGET_UPDATE:-0}

scala Finder "$SPARK_HOME" "$TMP2_DIR" "$BDTARGET_DIR" "$BDTARGET_UPDATED_DIR" "$BDTARGET_UPDATE"

echo $?
