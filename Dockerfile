# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Copy the build.gradle and settings.gradle files
COPY build.gradle settings.gradle ./

# Copy the Gradle wrapper files
COPY gradlew gradlew.bat ./
COPY gradle ./gradle

# Copy the source code
COPY src ./src

# Copy any additional scripts (e.g., generateJavaBindings.sh)
COPY generateJavaBindings.sh ./

# Run the Gradle build to create the application JAR
RUN ./gradlew build --no-daemon

# Expose the port the app runs on (if applicable)
EXPOSE 8080

# Set the default command to run the application
CMD ["java", "-cp", "build/libs/java-mmaped-proto-cache.jar", "org.example.App"]