FROM alpine:3.15

ARG kafka_version=3.3.1
ARG scala_version=2.13

ENV KAFKA_VERSION=$kafka_version \
    SCALA_VERSION=$scala_version \
    KAFKA_HOME=/opt/kafka

ENV PATH=${PATH}:${KAFKA_HOME}/bin

RUN wget https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz \
&& mv kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz /tmp/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz

COPY start-kafka.sh /tmp/

RUN apk add openjdk11 docker bash

RUN    chmod a+x /tmp/*.sh \
    && mv /tmp/start-kafka.sh /usr/bin \
    && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt \
    && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME}

#COPY server.properties $KAFKA_HOME/config

VOLUME ["/kafka"]

CMD start-kafka.sh
#CMD ["ls", "/opt/kafka/kafka_2.13-2.6.0"]