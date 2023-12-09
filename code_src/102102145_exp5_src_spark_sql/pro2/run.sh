#!/bin/bash

mvn package -DskipTests

if [[ $? -gt 0 ]] ; then
  exit 1
fi

mkdir -p output
rm -rf output/result.txt
$SPARK_HOME/bin/spark-submit --class net.homework.App \
  ./target/pro2-1.0-SNAPSHOT.jar | tee output/result.txt
