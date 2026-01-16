# ---------- Build stage ----------
FROM maven:3.9.9-eclipse-temurin-21 AS builder
WORKDIR /app

# Copy only pom.xml first for caching dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the fat JAR (skip tests to speed up)
RUN mvn clean package -DskipTests

# ---------- Runtime stage ----------
FROM eclipse-temurin:21-jre
WORKDIR /app

# Copy the built JAR from the build stage
COPY --from=builder /app/target/key-value-store-shard-1.0-SNAPSHOT.jar app.jar

# Expose your server port (change if your server uses a different one)
EXPOSE 8080

# Run the app
ENTRYPOINT ["java", "-jar", "app.jar"]
