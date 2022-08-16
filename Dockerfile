FROM maven:3.8.6-eclipse-temurin-17 as build
COPY /src /app/src
COPY pom.xml /app
RUN mvn -f /app/pom.xml clean package -DskipTests

FROM openjdk:17

RUN mkdir /app
COPY --from=build /app/target/kafka-admin-boot-0.0.2-SNAPSHOT.jar /app

CMD ["java","-jar","/app/kafka-admin-boot-0.0.2-SNAPSHOT.jar"]