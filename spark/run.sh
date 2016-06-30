#/bin/bash

$SPARK_HOME/bin/spark-submit \
    --class "com.deepera.ctg.ShortestPath" \
    --master local[4] \
    target/scala-2.11/shortestpath_2.11-0.0.1-SNAPSHOT.jar \
    /home/liuhao/Dropbox/iProject/CTG/workspace/CTG/sample/sample_100_virtual \
    /home/liuhao/Dropbox/iProject/CTG/workspace/CTG/sample/sample_100_vertices
