FROM alpine:3.15.0

ARG kafka_version=3.0.0
ARG scala_version=2.13

ENV KAFKA_VERSION=$kafka_version \
    SCALA_VERSION=$scala_version \
    KAFKA_HOME=/opt/kafka

ENV PATH=${PATH}:${KAFKA_HOME}/bin

COPY start-kafka.sh /tmp/
COPY kafka_2.13-3.0.0.tgz /tmp/

RUN apk add openjdk11 docker bash \
    && chmod a+x /tmp/*.sh \
    && mv /tmp/start-kafka.sh /usr/bin \
    && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt \
    && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME}

#COPY server.properties $KAFKA_HOME/config

VOLUME ["/kafka"]

CMD start-kafka.sh
#CMD ["ls", "/opt/kafka/kafka_2.13-2.6.0"]