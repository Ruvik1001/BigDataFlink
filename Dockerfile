FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY flink-job/pom.xml .
COPY flink-job/src ./src
RUN mvn -q -DskipTests package

FROM flink:1.19.1-scala_2.12-java17
COPY --from=build /src/target/star-stream.jar /opt/job/star-stream.jar
