# Extend vert.x base image
FROM vertx/vertx3

ENV VERTICLE_JAR kafka-samples-1.0-SNAPSHOT-fat.jar
ENV VERTICLE_HOME /usr/verticles
ENV VERTICLE_NAME davidul.cluster.Main

EXPOSE 8080 7896

# Copy the jar file into a container folder
COPY $VERTICLE_JAR $VERTICLE_HOME/
COPY wait-and-start.sh /
RUN chmod a+x /wait-and-start.sh

WORKDIR $VERTICLE_HOME

# Launch the verticle using 'java -jar verticles/vertx-kube-example-1.1.jar' command
#ENTRYPOINT ["sh", "-c", "java -jar $VERTICLE_HOME/$VERTICLE_JAR"]
ENTRYPOINT ["/wait-and-start.sh"]