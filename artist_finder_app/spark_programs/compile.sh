#!/bin/bash

for d in */ ; do
    cd "$d" && sbt clean; sbt package && cd ..
done
