FROM openjdk:21-jdk-slim

WORKDIR /app

COPY target/kafka-otlp-transformer.jar app.jar
COPY config.properties .

ENTRYPOINT ["java", "-jar", "app.jar"]
