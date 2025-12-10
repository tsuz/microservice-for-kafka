# Build stage
FROM eclipse-temurin:17-jdk-jammy AS build

WORKDIR /app
COPY . /app
RUN ./gradlew shadowJar

# Runtime stage
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Copy the built JAR file from the build stage
COPY --from=build /app/build/libs/kafka-as-a-microservice-standalone-*.jar /app/app.jar

# Create directories for configurations and monitoring
RUN mkdir -p /app/config /app/monitoring

# Copy monitoring files
COPY monitoring/jmx_prometheus_javaagent-1.0.1.jar /app/monitoring/
COPY monitoring/kafka_streams.yml /app/monitoring/

# Copy the start script
COPY scripts/docker_start.sh /app/scripts/

RUN chmod +x /app/scripts/docker_start.sh

EXPOSE 7001 7777

# Use the start script as entrypoint
ENTRYPOINT ["/app/scripts/docker_start.sh"]