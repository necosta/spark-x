FROM mesosphere/spark:latest

ARG CLASS_PATH
ARG JAR_PATH

ENV CLASS_PATH=$CLASS_PATH
ENV JAR_PATH=$JAR_PATH

ENTRYPOINT ./bin/spark-submit \
       --class $CLASS_PATH \
       --master local[2] \
       $JAR_PATH \
       100
