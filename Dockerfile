FROM mesosphere/spark:latest

ARG VERSION

ENV CLASS_PATH="pt.necosta.sparkx.SparkX"
ENV VERSION=$VERSION

COPY target/scala-2.11/sparkx_2.11-$VERSION.jar /opt

RUN mkdir /sparkx

# Manually copying the file to this project folder
# ToDO: Download source file from URL
COPY transportData.csv /sparkx/sourceData.csv

ENTRYPOINT ./bin/spark-submit \
       --class $CLASS_PATH \
       --master local[4] \
       /opt/sparkx_2.11-$VERSION.jar
