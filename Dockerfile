FROM openjdk:17

RUN mkdir /app
COPY target/kafka-admin-boot-0.0.2-SNAPSHOT.jar /app

CMD ["java","-jar","/app/kafka-admin-boot-0.0.2-SNAPSHOT.jar"]