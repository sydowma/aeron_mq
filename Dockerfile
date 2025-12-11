# Build stage
FROM eclipse-temurin:25-jdk AS builder

WORKDIR /app

# Copy Maven wrapper and pom files
COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .
COPY aeron-mq-protocol/pom.xml aeron-mq-protocol/
COPY aeron-mq-broker/pom.xml aeron-mq-broker/
COPY aeron-mq-gateway/pom.xml aeron-mq-gateway/
COPY aeron-mq-server/pom.xml aeron-mq-server/

# Download dependencies (cached layer)
RUN chmod +x mvnw && ./mvnw dependency:go-offline -B

# Copy source code
COPY aeron-mq-protocol/src aeron-mq-protocol/src
COPY aeron-mq-broker/src aeron-mq-broker/src
COPY aeron-mq-gateway/src aeron-mq-gateway/src
COPY aeron-mq-server/src aeron-mq-server/src

# Build the application
RUN ./mvnw package -DskipTests -B

# Runtime stage
FROM eclipse-temurin:25-jre

# Install necessary tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built JAR
COPY --from=builder /app/aeron-mq-server/target/aeron-mq-server-1.0.0-SNAPSHOT.jar /app/aeron-mq.jar

# Create data directories
RUN mkdir -p /app/data /app/logs

# Environment variables with defaults
ENV NODE_ID=0 \
    HOST=0.0.0.0 \
    KAFKA_PORT=9092 \
    DATA_DIR=/app/data \
    AERON_DIR=/dev/shm/aeron-mq \
    AUTO_CREATE_TOPICS=true \
    JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=10"

# Expose Kafka protocol port
EXPOSE 9092

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD nc -z localhost 9092 || exit 1

# Use dumb-init as PID 1 for proper signal handling
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Start the server
CMD ["sh", "-c", "java $JAVA_OPTS --enable-preview -jar /app/aeron-mq.jar"]

