# Build stage
FROM openjdk:17-jdk-slim AS build

WORKDIR /app
COPY . /app
RUN ./gradlew shadowJar

# Runtime stage
FROM openjdk:17-jdk-slim

WORKDIR /app

# Copy the built JAR file from the build stage
COPY --from=build /app/build/libs/kafka-as-a-microservice-standalone-*.jar /app/app.jar

# Create a directory for configurations
RUN mkdir -p /app/config

# Copy the start script
# COPY start.sh /app/scripts/docker_start.sh
COPY scripts/docker_start.sh /app/scripts/

RUN chmod +x /app/scripts/docker_start.sh

EXPOSE 7001

# Use the start script as entrypoint
ENTRYPOINT ["/app/scripts/docker_start.sh"]