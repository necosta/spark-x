FROM mesosphere/spark:latest

ARG VERSION

ENV SCALA_VERSION=2.11
ENV CLASS_PATH="pt.necosta.sparkx.SparkX"
ENV VERSION=$VERSION

COPY target/scala-$SCALA_VERSION/sparkx_$SCALA_VERSION-$VERSION.jar /opt

RUN mkdir /sparkx

# Manually copying the file to this project folder
# ToDO: Download source file from URL
COPY transportData.csv /sparkx/sourceData.csv

ENTRYPOINT ./bin/spark-submit \
       --class $CLASS_PATH \
       --master local[4] \
       /opt/sparkx_$SCALA_VERSION-$VERSION.jar
