FROM openjdk:21-jdk-slim

WORKDIR /app

COPY target/sevone-to-otlp-transformer-1.1-SNAPSHOT.jar app.jar
COPY config.properties .

ENTRYPOINT ["java", "-jar", "app.jar"]
