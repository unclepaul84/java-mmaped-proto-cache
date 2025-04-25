# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-jdk-slim

# Set the working directory in the container
WORKDIR /app

COPY build ./build

# Set the default command to run the application
CMD ["java", "-cp", "build/libs/java-mmaped-proto-cache-1.0.0.jar", "org.example.App"]