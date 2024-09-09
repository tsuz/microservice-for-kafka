# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-jdk-slim AS build

# Set the working directory in the container
WORKDIR /app

# Copy the project files into the container
COPY . /app

# Run the Gradle build and create the shadow JAR
RUN ./gradlew shadowJar

# Start a new stage for the runtime image
FROM openjdk:17-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Copy the built JAR file from the build stage
COPY --from=build /app/build/libs/kafka-as-a-microservice-standalone-*.jar /app/app.jar

# Copy the configuration file
COPY configuration/config.yaml /app/config.yaml

# Make port 7001 available to the world outside this container
EXPOSE 7001

# Run the jar file
CMD ["java", "-jar", "/app/app.jar", "/app/config.yaml"]