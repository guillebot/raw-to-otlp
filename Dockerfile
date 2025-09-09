FROM openjdk:21-jdk-slim

WORKDIR /app

COPY target/otlp-transformers-1.0.0.jar TransformRAWToOTLP.jar
#COPY config.properties .

ENTRYPOINT ["java", "-jar", "TransformRAWToOTLP.jar", "-c", "/config/config.yaml"]
